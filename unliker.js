#!/usr/bin/env node
/**
 * x-unliker — bulk unlike processor for X/Twitter
 * Version: 2.0.0 (hardened)
 *
 * Source of truth: like.js from Twitter data archive
 * Transport:       X internal GraphQL  /i/api/graphql/{queryId}/UnfavoriteTweet
 * Credentials:     cookies exported from logged-in browser session
 * Persistence:     state/ directory (JSON files + NDJSON log, atomic writes)
 * Rate limiting:   driven entirely by x-rate-limit-* response headers
 *
 * Usage:
 *   node unliker.js init --likes ./like.js --cookies ./cookies.json
 *   node unliker.js run [--dry-run] [--max <n>]
 *   node unliker.js status
 *   node unliker.js retry-failed
 *   node unliker.js reset
 */

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const os = require('os');
const readline = require('readline');

// CONFIGURATION

const CONFIG = {
    // Paths
    STATE_DIR: './state',
    PENDING_FILE: './state/pending.json',
    COMPLETED_FILE: './state/completed.ndjson',
    FAILED_FILE: './state/failed.json',
    CHECKPOINT_FILE: './state/checkpoint.json',
    COOKIES_FILE: './cookies.json',
    LOG_FILE: './state/run.log',

    // Rate limiting — driven by response headers; these are safe fallback defaults
    // X GraphQL limit: 500 per 15-minute fixed window
    RATE_LIMIT_WINDOW_MS: 15 * 60 * 1000,   // 900,000 ms
    DEFAULT_INTERVAL_MS: 4500,              // ~200 requests/window (40% of limit)
    MIN_INTERVAL_MS: 1000,              // never faster than 1 req/s
    MAX_INTERVAL_MS: 60_000,            // sanity cap — never slower than 1 req/min
    RATE_HEADROOM_REQUESTS: 20,                // pause when fewer than 20 remaining
    RATE_RESET_BUFFER_MS: 2000,              // extra cushion after reset timestamp

    // Retry
    MAX_ATTEMPTS_PER_TWEET: 3,
    RETRY_BACKOFF_BASE_MS: 5000,
    RETRY_BACKOFF_MAX_MS: 60_000,
    RETRY_JITTER_MAX_MS: 2000,             // max random jitter added to backoff

    // Circuit breaker — consecutive unexpected errors before halting
    CIRCUIT_BREAKER_THRESHOLD: 20,

    // 403 handling — retry this many times before giving up
    MAX_403_RETRIES: 4,
    BACKOFF_403_BASE_MS: 3000,

    // Checkpointing
    CHECKPOINT_EVERY_N: 50,               // save checkpoint metadata every N unlikes

    // Completed log batching
    COMPLETED_BATCH_SIZE: 25,               // flush completed entries every N
    COMPLETED_FLUSH_MS: 10_000,           // or every 10 seconds, whichever comes first

    // X API
    GRAPHQL_HOST: 'x.com',
    GRAPHQL_PATH_TPL: '/i/api/graphql/{queryId}/UnfavoriteTweet',
    BEARER_TOKEN: null,                   // loaded from cookies.json
    QUERY_ID: null,                   // loaded from checkpoint, updated on recovery

    // Safety
    DRY_RUN: false,
    MAX_PER_RUN: null,                        // null = unlimited
};

// LOGGING

let logStream = null;

function initLog() {
    fs.mkdirSync(CONFIG.STATE_DIR, { recursive: true });
    logStream = fs.createWriteStream(CONFIG.LOG_FILE, { flags: 'a' });
    logStream.on('error', (err) => {
        // Log stream errors must not crash the process
        console.error(`[LOG STREAM ERROR] ${err.message}`);
    });
}

function log(level, msg, data) {
    const ts = new Date().toISOString();
    const line = data
        ? `${ts} [${level}] ${msg} ${JSON.stringify(data)}`
        : `${ts} [${level}] ${msg}`;
    console.log(line);
    if (logStream) {
        try { logStream.write(line + '\n'); } catch (_) { }
    }
}

const logger = {
    info: (m, d) => log('INFO ', m, d),
    warn: (m, d) => log('WARN ', m, d),
    error: (m, d) => log('ERROR', m, d),
    debug: (m, d) => log('DEBUG', m, d),
};

// COMPLETED LOG — BATCHED WRITE BUFFER
//
// Instead of one appendFileSync per entry, we buffer entries in memory
// and flush periodically. This eliminates 134k individual syscalls and
// makes the tool resilient to slow / antivirus-scanned storage.
//
// Invariant: on any clean or signal-driven shutdown, flushCompletedBuffer()
// is called before process.exit. The only data loss window is an OS-level
// crash / power failure between flushes (max COMPLETED_BATCH_SIZE entries).

const completedBuffer = [];
let completedFlushTimer = null;

function scheduleCompletedFlush() {
    if (completedFlushTimer) return;
    completedFlushTimer = setTimeout(() => {
        flushCompletedBuffer();
    }, CONFIG.COMPLETED_FLUSH_MS);
    completedFlushTimer.unref(); // don't keep process alive just for this timer
}

function flushCompletedBuffer() {
    if (completedFlushTimer) {
        clearTimeout(completedFlushTimer);
        completedFlushTimer = null;
    }
    if (completedBuffer.length === 0) return;

    const lines = completedBuffer.splice(0).map(e => JSON.stringify(e)).join('\n') + '\n';
    try {
        fs.appendFileSync(CONFIG.COMPLETED_FILE, lines, 'utf8');
    } catch (err) {
        // Disk-full or permission error — log loudly, put entries back in buffer
        // so they survive the next flush attempt (or are reported at shutdown).
        logger.error('DISK WRITE FAILURE on completed.ndjson — entries buffered for retry', {
            error: err.message,
            code: err.code,
            buffered: completedBuffer.length + lines.split('\n').length - 1,
        });
        if (err.code === 'ENOSPC') {
            logger.error('DISK FULL. Free up space and restart — progress is safe.');
        }
        // Put the unsaved lines back so they're written on next flush or shutdown
        const unsaved = lines.trim().split('\n').map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
        completedBuffer.unshift(...unsaved);
    }
}

function queueCompleted(entry) {
    completedBuffer.push(entry);
    if (completedBuffer.length >= CONFIG.COMPLETED_BATCH_SIZE) {
        flushCompletedBuffer();
    } else {
        scheduleCompletedFlush();
    }
}

// Read all completed IDs from the NDJSON log into a Set.
// Tolerates malformed lines (skips them) and reports how many were skipped.
function loadCompletedSet() {
    const completed = new Set();
    completed.activelyUnlikedCount = 0;
    completed.alreadyAbsentCount = 0;
    completed.noopCount = 0;

    if (!fs.existsSync(CONFIG.COMPLETED_FILE)) return completed;

    let raw;
    try {
        raw = fs.readFileSync(CONFIG.COMPLETED_FILE, 'utf8');
    } catch (err) {
        logger.warn('Failed to read completed.ndjson, initializing empty completed set', { error: err.message });
        return completed;
    }

    let malformed = 0;
    const lines = raw.split('\n');
    for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
            const entry = JSON.parse(trimmed);
            if (entry && typeof entry.id === 'string' && entry.id.length > 0) {
                completed.add(entry.id);
                if (entry.note === 'not_in_favorites' || entry.note === 'not_found') {
                    completed.alreadyAbsentCount++;
                } else if (entry.note === 'noop') {
                    completed.noopCount++;
                } else {
                    completed.activelyUnlikedCount++;
                }
            } else {
                malformed++;
            }
        } catch {
            malformed++;
        }
    }

    if (malformed > 0) {
        logger.warn('Skipped malformed entries in completed.ndjson', { count: malformed });
    }

    logger.info('Completed set loaded', { count: completed.size, malformed });
    return completed;
}

// ATOMIC FILE I/O
// Write to a temp file then rename — prevents corruption on crash mid-write.

function atomicWriteJSON(filePath, data) {
    const tmp = filePath + '.tmp.' + process.pid;
    try {
        fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tmp, filePath);
    } catch (err) {
        // Clean up temp file if rename failed
        try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch (_) { }
        if (err.code === 'ENOSPC') {
            logger.error('Atomic write failed: Disk full', {
                file: filePath,
                error: err.message,
            });
        } else {
            logger.error('Atomic write failed', { file: filePath, error: err.message });
        }
        throw err; // re-throw so callers can handle
    }
}

function readJSONSafe(filePath, defaultValue) {
    try {
        if (!fs.existsSync(filePath)) return defaultValue;
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (e) {
        logger.warn(`Failed to parse ${filePath}, using default state`, { error: e.message });
        return defaultValue;
    }
}

// CREDENTIAL MANAGEMENT
//
// cookies.json format:
// {
// "bearerToken": "AAAAAAAAAA...",
// "queryId":     "ZYKSe-w7KEslx3JhSIk5LA",
// "cookies": {
// "auth_token": "...",
// "ct0":        "...",
// "guest_id":   "..."   (optional)
// }
// }

function protectCredentialsFile(filePath) {
    const platform = os.platform();
    if (platform === 'linux' || platform === 'darwin') {
        try {
            fs.chmodSync(filePath, 0o600);
            logger.info('Credentials file permissions set to 600');
        } catch (err) {
            logger.warn('Failed to chmod credentials file', { error: err.message });
        }
    } else if (platform === 'win32') {
        logger.warn(
            'SECURITY WARNING: Running on Windows. cookies.json contains sensitive credentials ' +
            '(equivalent to your Twitter password). Ensure this file is in a private directory ' +
            'not accessible to other users or processes.'
        );
    }
}

function loadCredentials(filePath) {
    const fp = filePath || CONFIG.COOKIES_FILE;

    if (!fs.existsSync(fp)) {
        throw new Error(
            `cookies.json not found at ${fp}.\n` +
            `See "How to generate cookies.json" in the companion guide.\n` +
            `Then run: node unliker.js init --likes ./like.js --cookies ./cookies.json`
        );
    }

    const creds = readJSONSafe(fp, null);
    if (!creds) throw new Error(`${fp} is malformed JSON.`);

    const missing = [];
    if (!creds.bearerToken) missing.push('bearerToken');
    if (!creds.queryId) missing.push('queryId');
    if (!creds.cookies?.auth_token) missing.push('cookies.auth_token');
    if (!creds.cookies?.ct0) missing.push('cookies.ct0');

    if (missing.length > 0) {
        throw new Error(`cookies.json is missing required fields: ${missing.join(', ')}`);
    }

    CONFIG.BEARER_TOKEN = creds.bearerToken;
    CONFIG.QUERY_ID = creds.queryId;

    return creds;
}

function buildCookieHeader(cookiesObj) {
    return Object.entries(cookiesObj)
        .map(([k, v]) => `${k}=${v}`)
        .join('; ');
}

// LIKE.JS PARSER

function parseLikeJs(filePath) {
    let content;
    try {
        content = fs.readFileSync(filePath, 'utf8');
    } catch (err) {
        throw new Error(`Cannot read like.js at ${filePath}: ${err.message}`);
    }

    const allIds = [];

    // like.js format: window.YTD.like.partN = [ {like: {tweetId: "..."}}, ... ]
    // There can be multiple part assignments. Find each JSON array separately.
    // We look for the assignment pattern and extract the JSON array after '='.
    const assignmentRegex = /window\.YTD\.like\.part\d+\s*=\s*(\[[\s\S]*?\]);?\s*(?=window\.|$)/g;
    let matched = false;
    let m;

    while ((m = assignmentRegex.exec(content)) !== null) {
        matched = true;
        try {
            const entries = JSON.parse(m[1]);
            for (const entry of entries) {
                const id = entry?.like?.tweetId;
                if (id && typeof id === 'string' && /^\d{10,19}$/.test(id)) {
                    allIds.push(id);
                }
            }
        } catch (err) {
            logger.warn('Failed to parse part in like.js, skipping', { error: err.message });
        }
    }

    // Fallback: if the multi-part regex found nothing, try a simple slice from '['
    // This handles older single-part archive formats.
    if (!matched || allIds.length === 0) {
        logger.warn('Multi-part regex match failed, attempting single-array fallback parse');
        const start = content.indexOf('[');
        if (start !== -1) {
            try {
                const arr = JSON.parse(content.slice(start));
                for (const entry of arr) {
                    const id = entry?.like?.tweetId;
                    if (id && typeof id === 'string' && /^\d{10,19}$/.test(id)) {
                        allIds.push(id);
                    }
                }
            } catch (err) {
                throw new Error(`Failed to parse like.js: ${err.message}`);
            }
        }
    }

    if (allIds.length === 0) {
        throw new Error('No tweet IDs found in like.js. Check the file format.');
    }

    // Deduplicate — archive can contain duplicates
    const uniqueSet = new Set(allIds);
    const unique = [...uniqueSet];
    const dupeCount = allIds.length - unique.length;
    if (dupeCount > 0) {
        logger.info('Parsed and deduplicated like.js', { total: allIds.length, unique: unique.length, duplicates: dupeCount });
    } else {
        logger.info('Parsed like.js', { total: allIds.length, unique: unique.length });
    }

    return unique;
}

// RATE LIMIT STATE
// Persisted in checkpoint so mid-window restarts don't burst requests.

const rateState = {
    limit: 500,
    remaining: 500,
    resetAt: Date.now() + 900_000,   // estimated; overwritten by first response

    // Apply response headers
    update(headers) {
        const limit = headers['x-rate-limit-limit'];
        const remaining = headers['x-rate-limit-remaining'];
        const reset = headers['x-rate-limit-reset'];

        if (limit) this.limit = parseInt(limit, 10);
        if (remaining) this.remaining = parseInt(remaining, 10);
        if (reset) this.resetAt = parseInt(reset, 10) * 1000;
    },

    // Serialize for checkpoint
    toJSON() {
        return {
            limit: this.limit,
            remaining: this.remaining,
            resetAt: this.resetAt,
        };
    },

    // Restore from checkpoint — only if the reset window hasn't expired.
    // If the window has already passed, the rate state is stale — use defaults.
    fromJSON(obj) {
        if (!obj) return;
        if (obj.resetAt && obj.resetAt > Date.now()) {
            this.limit = obj.limit ?? 500;
            this.remaining = obj.remaining ?? 500;
            this.resetAt = obj.resetAt;
            logger.info('Rate limits loaded from checkpoint', {
                remaining: this.remaining,
                resetIn: `${Math.round((this.resetAt - Date.now()) / 1000)}s`,
            });
        } else {
            logger.info('Checkpoint rate limits stale; using defaults');
        }
    },

    // How many ms to wait before the next request.
    // Spreads remaining budget evenly over remaining window time.
    // Pauses until reset when budget is near-exhausted.
    nextWaitMs() {
        if (this.remaining <= CONFIG.RATE_HEADROOM_REQUESTS) {
            const waitMs = this.resetAt - Date.now() + CONFIG.RATE_RESET_BUFFER_MS;
            return Math.max(0, waitMs);
        }

        const windowRemainingMs = Math.max(1, this.resetAt - Date.now());
        const interval = Math.ceil(windowRemainingMs / this.remaining);

        return Math.min(
            CONFIG.MAX_INTERVAL_MS,
            Math.max(CONFIG.MIN_INTERVAL_MS, interval)
        );
    },

    describe() {
        const resetIn = Math.max(0, Math.round((this.resetAt - Date.now()) / 1000));
        return `${this.remaining}/${this.limit} remaining, resets in ${resetIn}s`;
    },
};

// HTTP — single GraphQL unlike request

function httpsRequest(options, bodyStr) {
    return new Promise((resolve, reject) => {
        const req = https.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => {
                resolve({
                    status: res.statusCode,
                    headers: res.headers,
                    body: data,
                });
            });
            res.on('error', reject);
        });
        req.on('error', reject);
        req.setTimeout(20_000, () => {
            req.destroy(new Error('Request timeout after 20s'));
        });
        if (bodyStr) req.write(bodyStr);
        req.end();
    });
}

async function apiUnlikeTweet(tweetId, creds) {
    if (CONFIG.DRY_RUN) {
        await wait(50);
        return {
            ok: true,
            status: 200,
            done: true,
            errors: null,
            headers: {
                'x-rate-limit-limit': '500',
                'x-rate-limit-remaining': String(Math.max(0, rateState.remaining - 1)),
                'x-rate-limit-reset': String(Math.floor(rateState.resetAt / 1000)),
            },
            raw: '{"data":{"unfavorite_tweet":"Done"}}',
        };
    }

    const body = JSON.stringify({
        variables: { tweet_id: tweetId },
        queryId: CONFIG.QUERY_ID,
    });

    const cookieHeader = buildCookieHeader(creds.cookies);

    // Use the full UA string from a real Chrome session for authenticity.
    // The static string below is intentionally generic — replace with the
    // exact value from your browser's DevTools > Network > request headers.
    const ua =
        creds.userAgent ||
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

    const options = {
        hostname: CONFIG.GRAPHQL_HOST,
        path: CONFIG.GRAPHQL_PATH_TPL.replace('{queryId}', CONFIG.QUERY_ID),
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Content-Length': Buffer.byteLength(body),
            'Authorization': `Bearer ${CONFIG.BEARER_TOKEN}`,
            'Cookie': cookieHeader,
            'x-csrf-token': creds.cookies.ct0,
            'x-twitter-auth-type': 'OAuth2Session',
            'x-twitter-active-user': 'yes',
            'x-twitter-client-language': 'en',
            'User-Agent': ua,
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://x.com/',
            'Origin': 'https://x.com',
        },
    };

    const resp = await httpsRequest(options, body);

    let parsed = null;
    try { parsed = JSON.parse(resp.body); } catch { }

    const done = parsed?.data?.unfavorite_tweet === 'Done';
    const errors = parsed?.errors ?? null;

    return {
        ok: resp.status === 200 && done,
        status: resp.status,
        done,
        errors,
        headers: resp.headers,
        raw: resp.body,
    };
}

// STALE QUERY-ID DETECTION

function isStaleQueryIdError(result) {
    if (result.status === 400 || result.status === 404) return true;
    if (result.errors && result.status !== 200) {
        for (const e of result.errors) {
            // 34 = page not found, 214 = requires feature flag / endpoint changed
            if (e.code === 34 || e.code === 214) return true;
        }
    }
    return false;
}

async function promptNewQueryId() {
    logger.warn('');
    logger.warn('════════════════════════════════════════════════════');
    logger.warn('  STALE QUERY ID DETECTED');
    logger.warn('  X has likely deployed an update.');
    logger.warn('  How to get the new queryId:');
    logger.warn('  1. Open x.com in Chrome with DevTools (F12) open');
    logger.warn('  2. Go to the Network tab, filter by "graphql"');
    logger.warn('  3. Unlike any tweet manually on the page');
    logger.warn('  4. Find the "UnfavoriteTweet" request');
    logger.warn('  5. Copy the ID segment from the URL:');
    logger.warn('     /i/api/graphql/>>>THIS_PART<<</UnfavoriteTweet');
    logger.warn('════════════════════════════════════════════════════');
    logger.warn('');

    return new Promise((resolve) => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
        rl.question('Enter new queryId (or press Enter to abort): ', (answer) => {
            rl.close();
            const trimmed = answer.trim();
            resolve(trimmed || null);
        });
    });
}

// SESSION EXPIRY — 401 HANDLING

function printSessionExpiredInstructions() {
    logger.error('');
    logger.error('════════════════════════════════════════════════════');
    logger.error('  SESSION EXPIRED (401 Unauthorized)');
    logger.error('');
    logger.error('  Your Twitter session has expired or been revoked.');
    logger.error('  Progress is fully preserved — resume after refresh.');
    logger.error('');
    logger.error('  To refresh your session:');
    logger.error('  1. Open x.com in Chrome and ensure you are logged in');
    logger.error('  2. Open DevTools → Application → Cookies → https://x.com');
    logger.error('  3. Copy the new values for: auth_token, ct0');
    logger.error('  4. Open cookies.json and update those two fields');
    logger.error('  5. While in DevTools → Network, unlike a tweet');
    logger.error('     and confirm the queryId is still the same');
    logger.error('  6. Run: node unliker.js run');
    logger.error('');
    logger.error('  Your cookies.json must have this structure:');
    logger.error('  {');
    logger.error('    "bearerToken": "AAAAAAAAA...",');
    logger.error('    "queryId":     "ZYKSe-...",');
    logger.error('    "cookies": {');
    logger.error('      "auth_token": "<new value>",');
    logger.error('      "ct0":        "<new value>"');
    logger.error('    }');
    logger.error('  }');
    logger.error('════════════════════════════════════════════════════');
    logger.error('');
}

// CHECKPOINT MANAGEMENT

function loadCheckpoint() {
    return readJSONSafe(CONFIG.CHECKPOINT_FILE, {
        lastTweetId: null,
        completedCount: 0,
        failedCount: 0,
        skippedCount: 0,
        queryId: null,
        startedAt: null,
        updatedAt: null,
        runCount: 0,
        rateState: null,   // persisted rate limit state
    });
}

function saveCheckpoint(cp) {
    cp.updatedAt = new Date().toISOString();
    cp.rateState = rateState.toJSON();
    try {
        atomicWriteJSON(CONFIG.CHECKPOINT_FILE, cp);
    } catch (err) {
        logger.error('Failed to save checkpoint', { error: err.message });
        // Non-fatal — we continue running; next checkpoint attempt may succeed
    }
}

// UTILITIES

function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(maxMs) {
    return Math.floor(Math.random() * maxMs);
}

function recordFailure(failedMap, tweetId, reason) {
    if (!failedMap[tweetId]) {
        failedMap[tweetId] = { attempts: 0, lastError: null, lastAttemptAt: null };
    }
    failedMap[tweetId].attempts++;
    failedMap[tweetId].lastError = reason;
    failedMap[tweetId].lastAttemptAt = new Date().toISOString();
}

function safeWriteJSON(filePath, data, label) {
    try {
        atomicWriteJSON(filePath, data);
    } catch (err) {
        logger.error(`Failed to write ${label || filePath}`, { error: err.message });
    }
}

// CIRCUIT BREAKER

const circuitBreaker = {
    consecutiveUnexpectedErrors: 0,

    reset() {
        this.consecutiveUnexpectedErrors = 0;
    },

    recordError() {
        this.consecutiveUnexpectedErrors++;
    },

    isOpen() {
        return this.consecutiveUnexpectedErrors >= CONFIG.CIRCUIT_BREAKER_THRESHOLD;
    },

    describe() {
        return `${this.consecutiveUnexpectedErrors}/${CONFIG.CIRCUIT_BREAKER_THRESHOLD} consecutive errors`;
    },
};

// MAIN QUEUE PROCESSOR

async function runProcessor(creds) {
    // Load state
    const checkpoint = loadCheckpoint();
    const completedSet = loadCompletedSet();
    const failedMap = readJSONSafe(CONFIG.FAILED_FILE, {});
    const pending = readJSONSafe(CONFIG.PENDING_FILE, []);

    if (pending.length === 0) {
        logger.info('No pending tweets found. Check status using "status" command.');
        return;
    }

    // Restore queryId from checkpoint if newer
    if (checkpoint.queryId && checkpoint.queryId !== CONFIG.QUERY_ID) {
        logger.info('Query ID loaded from checkpoint', { queryId: checkpoint.queryId });
        CONFIG.QUERY_ID = checkpoint.queryId;
        creds.queryId = checkpoint.queryId;
    }

    // Restore rate state from checkpoint
    rateState.fromJSON(checkpoint.rateState);

    // Shutdown flag — set by SIGINT/SIGTERM, checked each loop iteration
    let shutdownRequested = false;

    const handleSignal = (signal) => {
        logger.info(`Signal ${signal} received; finalizing active request and saving state`);
        shutdownRequested = true;
    };

    // Use once() so the handler doesn't stack on repeated Ctrl+C
    process.once('SIGINT', () => handleSignal('SIGINT'));
    process.once('SIGTERM', () => handleSignal('SIGTERM'));

    checkpoint.runCount = (checkpoint.runCount || 0) + 1;
    checkpoint.startedAt = checkpoint.startedAt ?? new Date().toISOString();

    logger.info('Starting run...', {
        pending: pending.length,
        completed: completedSet.size,
        failed: Object.keys(failedMap).length,
        queryId: CONFIG.QUERY_ID,
        runCount: checkpoint.runCount,
        dryRun: CONFIG.DRY_RUN,
    });

    // Build effective processing array
    // Filter out already-completed entries using the completed log as source of truth.
    const pendingArray = pending.filter((id) => !completedSet.has(id));
    logger.info('Pending queue filtered against completed entries', { count: pendingArray.length });

    let pendingIndex = 0;
    let processedCount = 0; // tracks max_per_run
    let sessionUnliked = 0;
    let sessionAlreadyAbsent = 0;
    let sessionNoop = 0;
    let sessionFailed = 0;
    let staleQueryCount = 0;
    let consec403 = 0;   // consecutive 403s before giving up

    // Main loop
    while (pendingIndex < pendingArray.length) {

        // Check shutdown flag
        if (shutdownRequested) {
            logger.info('Shutdown signal received; persisting state and exiting');
            break;
        }

        // Circuit breaker
        if (circuitBreaker.isOpen()) {
            logger.error(
                `Circuit breaker tripped after ${circuitBreaker.describe()}. ` +
                'Something is systematically wrong. Saving state and stopping.'
            );
            logger.error('Check the last few ERROR lines above. Common causes:');
            logger.error('  - X is returning unexpected error formats (platform issue)');
            logger.error('  - Your IP may be temporarily blocked');
            logger.error('  - A network proxy is intercepting requests');
            break;
        }

        const tweetId = pendingArray[pendingIndex];

        // Idempotency double-check
        if (completedSet.has(tweetId)) {
            pendingIndex++;
            continue;
        }

        // MAX_PER_RUN guard
        if (CONFIG.MAX_PER_RUN !== null && processedCount >= CONFIG.MAX_PER_RUN) {
            logger.info('MAX_PER_RUN limit reached; stopping execution', { limit: CONFIG.MAX_PER_RUN });
            break;
        }

        // Skip permanently-failed tweets
        const failEntry = failedMap[tweetId];
        if (failEntry && failEntry.attempts >= CONFIG.MAX_ATTEMPTS_PER_TWEET) {
            logger.warn('Skipping tweet: max attempts exceeded', { tweetId, attempts: failEntry.attempts });
            pendingIndex++;
            continue;
        }

        // Rate-limit wait (with verbose logging for long sleeps)
        const waitMs = rateState.nextWaitMs();
        if (waitMs > 0) {
            if (waitMs >= 60_000) {
                const waitMin = (waitMs / 60_000).toFixed(1);
                const resetTime = new Date(rateState.resetAt).toISOString();
                logger.info(
                    `Rate limit reached; pausing ${waitMin}m until window reset at ${resetTime}`,
                    { waitMs, rateState: rateState.describe() }
                );
                // Log status periodically during long sleeps
                let slept = 0;
                while (slept < waitMs && !shutdownRequested) {
                    const chunk = Math.min(30_000, waitMs - slept);
                    await wait(chunk);
                    slept += chunk;
                    if (slept < waitMs && !shutdownRequested) {
                        const remaining = Math.round((waitMs - slept) / 1000);
                        logger.info(`Still paused: ${remaining}s remaining until window reset`);
                    }
                }
            } else if (waitMs >= 5_000) {
                logger.info(`Rate limit pause: ${Math.round(waitMs / 1000)}s`, { rate: rateState.describe() });
                await wait(waitMs);
            } else {
                await wait(waitMs);
            }
        }

        // Re-check shutdown after potentially long sleep
        if (shutdownRequested) break;

        // Make the API request
        let result;
        try {
            result = await apiUnlikeTweet(tweetId, creds);
        } catch (networkErr) {
            logger.error('Network error', { tweetId, error: networkErr.message });
            recordFailure(failedMap, tweetId, `NETWORK: ${networkErr.message}`);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            circuitBreaker.recordError();
            await wait(5000 + jitter(2000));
            // Don't advance index — retry this tweet
            continue;
        }

        // Update rate state from response headers
        rateState.update(result.headers);

        // RESPONSE HANDLING

        // SUCCESS
        if (result.ok) {
            completedSet.add(tweetId);
            queueCompleted({ id: tweetId, ts: Math.floor(Date.now() / 1000) });
            delete failedMap[tweetId];

            sessionUnliked++;
            processedCount++;
            pendingIndex++;
            staleQueryCount = 0;   // reset stale-query counter on success
            consec403 = 0;
            circuitBreaker.reset();

            checkpoint.completedCount = completedSet.size;
            checkpoint.lastTweetId = tweetId;
            checkpoint.queryId = CONFIG.QUERY_ID;

            if (processedCount % CONFIG.CHECKPOINT_EVERY_N === 0) {
                // Flush completed buffer so checkpoint reflects on-disk state
                flushCompletedBuffer();
                saveCheckpoint(checkpoint);

                // Rewrite pending.json every 500 to trim completed entries from it.
                // This keeps the file size reasonable and speeds up future init calls.
                if (processedCount % 500 === 0) {
                    const remaining = pendingArray.slice(pendingIndex);
                    safeWriteJSON(CONFIG.PENDING_FILE, remaining, 'pending.json');
                    logger.info('Checkpoint persisted; pending queue compacted', {
                        processed: processedCount,
                        completed: completedSet.size,
                        remaining: remaining.length,
                        rate: rateState.describe(),
                    });
                } else {
                    logger.info('Checkpoint persisted', {
                        processed: processedCount,
                        completed: completedSet.size,
                        rate: rateState.describe(),
                    });
                }
            }

            continue;
        }

        // RATE LIMITED (429)
        if (result.status === 429) {
            const resetIn = Math.max(0, rateState.resetAt - Date.now() + CONFIG.RATE_RESET_BUFFER_MS);
            const resetMin = (resetIn / 60_000).toFixed(1);
            logger.warn(
                'Rate limit (429) hit, backing off',
                { resetAt: new Date(rateState.resetAt).toISOString(), waitMin: resetMin }
            );
            // Log status periodically during the wait
            let slept = 0;
            while (slept < resetIn && !shutdownRequested) {
                const chunk = Math.min(30_000, resetIn - slept);
                await wait(chunk);
                slept += chunk;
                if (slept < resetIn && !shutdownRequested) {
                    logger.info(`Rate limit wait: ${Math.round((resetIn - slept) / 1000)}s remaining`);
                }
            }
            // Don't advance index — retry same tweet after wait
            continue;
        }

        // AUTH FAILED (401)
        if (result.status === 401) {
            flushCompletedBuffer();
            saveCheckpoint(checkpoint);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            printSessionExpiredInstructions();
            process.exit(1);
        }

        // CSRF / SESSION ISSUE (403)
        // Retry several times with exponential backoff + jitter before aborting.
        if (result.status === 403) {
            consec403++;
            const backoffMs = Math.min(
                CONFIG.BACKOFF_403_BASE_MS * Math.pow(2, consec403 - 1),
                CONFIG.RETRY_BACKOFF_MAX_MS
            ) + jitter(CONFIG.RETRY_JITTER_MAX_MS);

            logger.warn(`403 Forbidden: attempt ${consec403}/${CONFIG.MAX_403_RETRIES}`, {
                body: result.raw.slice(0, 200),
                backoffMs,
            });

            if (consec403 < CONFIG.MAX_403_RETRIES) {
                // Try refreshing ct0 from cookies.json in case the user updated it externally
                const freshCreds = readJSONSafe(CONFIG.COOKIES_FILE, null);
                if (freshCreds?.cookies?.ct0 && freshCreds.cookies.ct0 !== creds.cookies.ct0) {
                    logger.info('ct0 refreshed from cookies.json');
                    creds.cookies.ct0 = freshCreds.cookies.ct0;
                }
                await wait(backoffMs);
                continue; // retry same tweet
            }

            // MAX_403_RETRIES exhausted
            logger.error('CSRF/session token authentication failed repeatedly, aborting', { retries: CONFIG.MAX_403_RETRIES });
            logger.error('Run: node unliker.js run   (after updating cookies.json)');
            flushCompletedBuffer();
            saveCheckpoint(checkpoint);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            process.exit(1);
        }

        // Reset 403 counter on any non-403 response
        consec403 = 0;

        // STALE QUERY ID (400 / 404 + specific error codes)
        if (isStaleQueryIdError(result)) {
            staleQueryCount++;
            logger.warn('Possible stale query ID detected', { tweetId, attempt: staleQueryCount });

            if (staleQueryCount >= 3) {
                logger.error('Stale query ID confirmed after 3 consecutive errors');
                const newId = await promptNewQueryId();
                if (newId) {
                    CONFIG.QUERY_ID = newId;
                    creds.queryId = newId;
                    // Persist new queryId into cookies.json
                    const storedCreds = readJSONSafe(CONFIG.COOKIES_FILE, {});
                    storedCreds.queryId = newId;
                    safeWriteJSON(CONFIG.COOKIES_FILE, storedCreds, 'cookies.json');
                    checkpoint.queryId = newId;
                    saveCheckpoint(checkpoint);
                    staleQueryCount = 0;
                    circuitBreaker.reset();
                    logger.info('Query ID updated, resuming execution');
                } else {
                    logger.error('No query ID provided; persisting state and aborting');
                    flushCompletedBuffer();
                    saveCheckpoint(checkpoint);
                    safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
                    process.exit(1);
                }
            }
            // Retry same tweet
            await wait(2000 + jitter(1000));
            continue;
        }

        // TWEET NOT FOUND / ALREADY UNLIKED (200 but not Done)
        if (result.status === 200 && !result.done) {
            const errors = result.errors || [];
            const isCode144 = errors.some(e => e.code === 144);
            const isCode34 = errors.some(e => e.code === 34);

            let note = 'noop';
            if (isCode144) {
                note = 'not_in_favorites';
                sessionAlreadyAbsent++;
                logger.info(`Tweet ${tweetId} not found in favorites (code 144)`);
            } else if (isCode34) {
                note = 'not_found';
                sessionAlreadyAbsent++;
                logger.info(`Tweet ${tweetId} not found/deleted (code 34)`);
            } else {
                sessionNoop++;
                logger.warn(`Tweet ${tweetId} returned 200/not-Done (noop)`, { errors });
            }

            completedSet.add(tweetId);
            queueCompleted({ id: tweetId, ts: Math.floor(Date.now() / 1000), note });
            pendingIndex++;
            processedCount++;
            staleQueryCount = 0;
            circuitBreaker.reset();
            checkpoint.completedCount = completedSet.size;
            continue;
        }

        // UNKNOWN / UNEXPECTED ERROR
        logger.warn('Unexpected response structure', { tweetId, status: result.status, body: result.raw.slice(0, 100) });
        recordFailure(failedMap, tweetId, `HTTP_${result.status}: ${result.raw.slice(0, 100)}`);
        safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
        circuitBreaker.recordError();
        pendingIndex++;
        sessionFailed++;
        processedCount++;
        checkpoint.failedCount = Object.keys(failedMap).length;
    }

    // Final flush and save
    flushCompletedBuffer();
    const remaining = pendingArray.slice(pendingIndex);
    safeWriteJSON(CONFIG.PENDING_FILE, remaining, 'pending.json');
    saveCheckpoint(checkpoint);
    safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');

    // Remove signal handlers we added
    process.removeAllListeners('SIGINT');
    process.removeAllListeners('SIGTERM');

    logger.info('Run finished', {
        unliked: sessionUnliked,
        alreadyAbsent: sessionAlreadyAbsent,
        noop: sessionNoop,
        failed: sessionFailed,
        completed: completedSet.size,
        remaining: remaining.length,
        failedQueueCount: Object.keys(failedMap).length,
        circuitOpen: circuitBreaker.isOpen(),
        shutdownByUser: shutdownRequested,
    });

    if (remaining.length === 0 && Object.keys(failedMap).length === 0) {
        logger.info('All tweets processed successfully.');
    } else if (remaining.length > 0) {
        logger.info('Run: node unliker.js run (to continue)');
    }
    if (Object.keys(failedMap).length > 0) {
        logger.info('Run: node unliker.js retry-failed (to retry failed tweets)');
    }
}

// COMMANDS

async function cmdInit(args) {
    const likesFlag = args.indexOf('--likes');
    const cookiesFlag = args.indexOf('--cookies');

    if (likesFlag === -1 || cookiesFlag === -1) {
        console.error('Usage: node unliker.js init --likes ./like.js --cookies ./cookies.json');
        process.exit(1);
    }

    const likesFile = args[likesFlag + 1];
    const cookiesFile = args[cookiesFlag + 1];

    if (!likesFile || !fs.existsSync(likesFile)) {
        console.error(`like.js not found: ${likesFile}`);
        process.exit(1);
    }
    if (!cookiesFile || !fs.existsSync(cookiesFile)) {
        console.error(`cookies.json not found: ${cookiesFile}`);
        process.exit(1);
    }

    fs.mkdirSync(CONFIG.STATE_DIR, { recursive: true });

    // Parse like.js
    console.log('Parsing like.js...');
    const allIds = parseLikeJs(likesFile);
    console.log(`Found ${allIds.length} unique tweet IDs.`);

    // Load existing completed set to allow re-init without losing progress
    const completedSet = loadCompletedSet();
    const pending = allIds.filter((id) => !completedSet.has(id));
    console.log(`Already completed: ${completedSet.size}. Pending: ${pending.length}.`);

    // Copy cookies to canonical path if different
    const canonicalCookies = path.resolve(CONFIG.COOKIES_FILE);
    if (path.resolve(cookiesFile) !== canonicalCookies) {
        fs.copyFileSync(cookiesFile, canonicalCookies);
        console.log(`Credentials copied to ${CONFIG.COOKIES_FILE}`);
    }

    // Protect credentials file
    protectCredentialsFile(canonicalCookies);

    // Validate credentials
    const creds = loadCredentials();
    console.log(`Credentials loaded. queryId: ${creds.queryId}`);

    // Write pending.json
    atomicWriteJSON(CONFIG.PENDING_FILE, pending);
    console.log(`Wrote ${pending.length} IDs to ${CONFIG.PENDING_FILE}`);

    // Update checkpoint
    const cp = loadCheckpoint();
    cp.queryId = creds.queryId;
    cp.updatedAt = new Date().toISOString();
    saveCheckpoint(cp);

    console.log('\nInit complete.');
    console.log(`Run: node unliker.js run --max 10    (test with 10 tweets first)`);
    console.log(`Run: node unliker.js run              (full run)`);
}

async function cmdRun(args) {
    initLog();

    if (args.includes('--dry-run')) {
        CONFIG.DRY_RUN = true;
        logger.info('DRY RUN MODE — no actual requests will be made');
    }

    const maxFlag = args.indexOf('--max');
    if (maxFlag !== -1) {
        const n = parseInt(args[maxFlag + 1], 10);
        if (!isNaN(n) && n > 0) {
            CONFIG.MAX_PER_RUN = n;
            logger.info(`MAX_PER_RUN set to ${CONFIG.MAX_PER_RUN}`);
        } else {
            logger.error('--max requires a positive integer');
            process.exit(1);
        }
    }

    if (!fs.existsSync(CONFIG.PENDING_FILE)) {
        logger.error('No pending.json found. Run: node unliker.js init --likes ./like.js --cookies ./cookies.json');
        process.exit(1);
    }

    const creds = loadCredentials();
    protectCredentialsFile(path.resolve(CONFIG.COOKIES_FILE));

    await runProcessor(creds);
}

async function cmdStatus() {
    const checkpoint = loadCheckpoint();
    const completedSet = loadCompletedSet();
    const pending = readJSONSafe(CONFIG.PENDING_FILE, []);
    const failedMap = readJSONSafe(CONFIG.FAILED_FILE, {});

    // Compute effective pending (what the run loop would actually process)
    const effectivePending = pending.filter((id) => !completedSet.has(id));
    const total = completedSet.size + effectivePending.length;
    const pct = total > 0 ? ((completedSet.size / total) * 100).toFixed(1) : '0.0';

    console.log('\n═══════════════════════════════');
    console.log('  X Unliker — Status');
    console.log('═══════════════════════════════');
    console.log(`  Total IDs:     ${total}`);
    console.log(`  Completed:     ${completedSet.size} (${pct}%)`);
    console.log(`  Pending:       ${effectivePending.length}`);
    console.log(`  Failed:        ${Object.keys(failedMap).length}`);
    console.log(`  QueryId:       ${checkpoint.queryId ?? 'unknown'}`);
    console.log(`  Last tweet:    ${checkpoint.lastTweetId ?? 'none'}`);
    console.log(`  Runs:          ${checkpoint.runCount ?? 0}`);
    console.log(`  Last updated:  ${checkpoint.updatedAt ?? 'never'}`);

    if (checkpoint.rateState) {
        const rs = checkpoint.rateState;
        const resetIn = Math.max(0, Math.round((rs.resetAt - Date.now()) / 1000));
        const expired = rs.resetAt < Date.now() ? ' (window expired)' : '';
        console.log(`  Rate (saved):  ${rs.remaining}/${rs.limit} remaining, reset in ${resetIn}s${expired}`);
    }

    if (Object.keys(failedMap).length > 0) {
        console.log('\n  Failed tweet samples (first 5):');
        Object.entries(failedMap).slice(0, 5).forEach(([id, info]) => {
            console.log(`    ${id}: ${info.lastError} (${info.attempts} attempts)`);
        });
    }

    // ETA — computed from recent throughput in the completed log
    if (effectivePending.length > 0 && completedSet.size > 0 && fs.existsSync(CONFIG.COMPLETED_FILE)) {
        try {
            const raw = fs.readFileSync(CONFIG.COMPLETED_FILE, 'utf8');
            const lines = raw.split('\n').filter(Boolean);
            const recent = lines.slice(-1000);
            if (recent.length >= 2) {
                const first = JSON.parse(recent[0]);
                const last = JSON.parse(recent[recent.length - 1]);
                const elapsed = last.ts - first.ts;
                if (elapsed > 0) {
                    const ratePerSec = recent.length / elapsed;
                    const etaSec = effectivePending.length / ratePerSec;
                    const etaHours = (etaSec / 3600).toFixed(1);
                    const etaDays = (etaSec / 86400).toFixed(1);
                    console.log(`\n  Recent rate:   ${(ratePerSec * 3600).toFixed(0)} unlikes/hour`);
                    console.log(`  ETA (continuous): ${etaHours} hours (~${etaDays} days)`);
                }
            }
        } catch { }
    }

    console.log('');
}

async function cmdAnalytics() {
    if (!fs.existsSync(CONFIG.LOG_FILE)) {
        console.error(`Log file not found at ${CONFIG.LOG_FILE}`);
        process.exit(1);
    }

    const fileStream = fs.createReadStream(CONFIG.LOG_FILE);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let firstTimestamp = null;
    let lastTimestamp = null;

    let sessions = [];
    let currentSession = null;
    let totalAttempts = 0;
    let totalRateLimitPauses = 0;
    let totalRateLimitPauseTime = 0;

    // Regex patterns
    const logLineRegex = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\s+\[(\w+)\s*\]\s+(.*)$/;
    const startRunRegex = /Starting run(?:\.\.\.)?\s*(.*)$/;
    const endRunRegex = /Run (?:complete|finished)\s*(.*)$/;
    const longPauseRegex = /(?:pausing|waiting) ([\d.]+)\s*(?:minutes|m)/i;
    const shortPauseRegex = /rate limit pause:\s*(\d+)s/i;
    const attemptRegex = /(?:Tweet \d+ (?:returned|not found|already|not found\/deleted)|Network error|Unexpected response|403 Forbidden)/i;

    let lastSessionLineTime = null;

    for await (const line of rl) {
        const match = logLineRegex.exec(line);
        if (!match) continue;

        const timestampStr = match[1];
        const level = match[2];
        const message = match[3];

        const time = new Date(timestampStr).getTime();
        if (isNaN(time)) continue;

        if (!firstTimestamp) firstTimestamp = time;
        lastTimestamp = time;

        // Detect process boundaries or sleep periods by checking the gap between consecutive log lines.
        // If the gap is > 5 minutes, we treat the previous session as implicitly ended before the gap.
        if (currentSession && lastSessionLineTime) {
            const gap = time - lastSessionLineTime;
            if (gap > 5 * 60 * 1000) {
                currentSession.end = lastSessionLineTime;
                sessions.push(currentSession);
                currentSession = null;
            }
        }

        // Session handling
        const startMatch = startRunRegex.exec(message);
        if (startMatch) {
            if (currentSession) {
                // Previous session crashed or didn't end cleanly
                currentSession.end = lastSessionLineTime || time;
                sessions.push(currentSession);
            }
            currentSession = { start: time, end: null };
        }

        const endMatch = endRunRegex.exec(message);
        if (endMatch && currentSession) {
            currentSession.end = time;
            sessions.push(currentSession);
            currentSession = null;
        }

        if (currentSession) {
            lastSessionLineTime = time;

            // Check for rate-limit pauses inside active session
            const longMatch = longPauseRegex.exec(message);
            if (longMatch) {
                const mins = parseFloat(longMatch[1]);
                const ms = mins * 60 * 1000;
                totalRateLimitPauseTime += ms;
                totalRateLimitPauses++;
            } else {
                const shortMatch = shortPauseRegex.exec(message);
                if (shortMatch) {
                    const secs = parseInt(shortMatch[1], 10);
                    const ms = secs * 1000;
                    totalRateLimitPauseTime += ms;
                    totalRateLimitPauses++;
                }
            }

            // Check for tweet attempts
            if (attemptRegex.test(message)) {
                totalAttempts++;
            }
        }
    }

    // Handle last session if still active at EOF
    if (currentSession) {
        currentSession.end = lastSessionLineTime || lastTimestamp;
        sessions.push(currentSession);
    }

    if (sessions.length === 0) {
        console.log('No run sessions found in logs.');
        return;
    }

    // Calculations
    const wallClockDuration = lastTimestamp - firstTimestamp;
    let totalExecutionTime = 0;
    for (const session of sessions) {
        totalExecutionTime += (session.end - session.start);
    }

    const offlineTime = Math.max(0, wallClockDuration - totalExecutionTime);
    const activeProcessingTime = Math.max(0, totalExecutionTime - totalRateLimitPauseTime);

    const completedSet = loadCompletedSet();
    const totalCompleted = completedSet.size;

    const activeCompleted = completedSet.activelyUnlikedCount || 0;
    totalAttempts += activeCompleted;

    // Throughputs
    const activeThroughput = activeProcessingTime > 0 
        ? (totalAttempts / (activeProcessingTime / 3600000)) 
        : 0;
    const realWorldThroughput = wallClockDuration > 0 
        ? (totalCompleted / (wallClockDuration / 3600000)) 
        : 0;
    const avgRequestInterval = totalAttempts > 0 
        ? (activeProcessingTime / totalAttempts) 
        : 0;

    // Formatter helpers
    const formatDuration = (ms) => {
        const seconds = Math.floor(ms / 1000) % 60;
        const minutes = Math.floor(ms / (1000 * 60)) % 60;
        const hours = Math.floor(ms / (1000 * 60 * 60)) % 24;
        const days = Math.floor(ms / (1000 * 60 * 60 * 24));

        const parts = [];
        if (days > 0) parts.push(`${days}d`);
        if (hours > 0) parts.push(`${hours}h`);
        if (minutes > 0) parts.push(`${minutes}m`);
        if (parts.length === 0 || seconds > 0) parts.push(`${seconds}s`);

        return parts.join(' ');
    };

    console.log('\n════════════════════════════════════════════════════════════');
    console.log('  X Unliker — Project Runtime Analytics');
    console.log('════════════════════════════════════════════════════════════');
    console.log(`  Project Span:             ${formatDuration(wallClockDuration)}`);
    console.log(`    - First Log Entry:      ${new Date(firstTimestamp).toISOString()}`);
    console.log(`    - Final Log Entry:      ${new Date(lastTimestamp).toISOString()}`);
    console.log('');
    console.log(`  Session Statistics:`);
    console.log(`    - Run Sessions:         ${sessions.length}`);
    console.log(`    - Total Execution Time: ${formatDuration(totalExecutionTime)}`);
    console.log(`    - Offline Time:         ${formatDuration(offlineTime)}`);
    console.log(`      (Intervals between run sessions)`);
    console.log('');
    console.log(`  Rate Limiting & Pauses:`);
    console.log(`    - Rate-Limit Pauses:    ${totalRateLimitPauses}`);
    console.log(`    - Total Paused Time:    ${formatDuration(totalRateLimitPauseTime)}`);
    console.log(`    - Active Processing:    ${formatDuration(activeProcessingTime)}`);
    console.log('');
    console.log(`  Throughput & Efficiency:`);
    console.log(`    - Total Attempts:       ${totalAttempts.toLocaleString()}`);
    console.log(`    - Total Completed:      ${totalCompleted.toLocaleString()}`);
    console.log(`    - Active Rate:          ${activeThroughput.toFixed(1)} tweets/hour`);
    console.log(`    - Effective Rate:       ${realWorldThroughput.toFixed(1)} tweets/hour`);
    console.log(`      (Overall rate across total project span)`);
    console.log(`    - Avg Request Interval: ${avgRequestInterval.toFixed(0)} ms`);
    console.log('════════════════════════════════════════════════════════════\n');
}

async function cmdRetryFailed() {
    const failedMap = readJSONSafe(CONFIG.FAILED_FILE, {});
    const pending = readJSONSafe(CONFIG.PENDING_FILE, []);

    const toRetry = Object.keys(failedMap);
    if (toRetry.length === 0) {
        console.log('No failed tweets to retry.');
        return;
    }

    // Reset attempt counts.
    // Append to END of queue so failed tweets don't block normal processing.
    toRetry.forEach((id) => { delete failedMap[id]; });
    const pendingSet = new Set(pending);
    const toAdd = toRetry.filter((id) => !pendingSet.has(id));
    const newPending = [...pending, ...toAdd];

    atomicWriteJSON(CONFIG.PENDING_FILE, newPending);
    atomicWriteJSON(CONFIG.FAILED_FILE, {});

    console.log(`Moved ${toRetry.length} failed tweets to the END of the pending queue.`);
    console.log(`Run: node unliker.js run`);
}

async function cmdReset() {
    console.log('WARNING: This will archive all state (pending, completed, failed, checkpoint).');
    console.log('Your like.js file will NOT be affected.');
    console.log('Archived files will be saved to state/archive_<timestamp>/');

    const answer = await new Promise((resolve) => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
        rl.question('Type "RESET" to confirm: ', (ans) => { rl.close(); resolve(ans.trim()); });
    });

    if (answer !== 'RESET') {
        console.log('Aborted.');
        return;
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const archiveDir = path.join(CONFIG.STATE_DIR, `archive_${timestamp}`);
    fs.mkdirSync(archiveDir, { recursive: true });

    const files = [
        CONFIG.PENDING_FILE,
        CONFIG.COMPLETED_FILE,
        CONFIG.FAILED_FILE,
        CONFIG.CHECKPOINT_FILE,
    ];

    for (const f of files) {
        if (fs.existsSync(f)) {
            const dest = path.join(archiveDir, path.basename(f));
            fs.renameSync(f, dest);
            console.log(`  Archived: ${f} → ${dest}`);
        }
    }

    console.log(`\nState archived to: ${archiveDir}`);
    console.log(`Run: node unliker.js init --likes ./like.js --cookies ./cookies.json`);
}

// ENTRY POINT

async function main() {
    const args = process.argv.slice(2);
    const cmd = args[0];

    switch (cmd) {
        case 'init': await cmdInit(args.slice(1)); break;
        case 'run': await cmdRun(args.slice(1)); break;
        case 'status': await cmdStatus(); break;
        case 'retry-failed': await cmdRetryFailed(); break;
        case 'analytics': await cmdAnalytics(); break;
        case 'reset': await cmdReset(); break;
        default:
            console.log('Usage: node unliker.js <command> [options]');
            console.log('');
            console.log('Commands:');
            console.log('  init --likes <like.js> --cookies <cookies.json>');
            console.log('    Parse like.js and set up state for a new (or resumed) run.');
            console.log('');
            console.log('  run [--dry-run] [--max <n>]');
            console.log('    Process the pending queue. Always safe to re-run (idempotent).');
            console.log('    --dry-run   Simulate without making real requests.');
            console.log('    --max <n>   Stop after N unlikes (for testing).');
            console.log('');
            console.log('  status');
            console.log('    Show current progress, rate, ETA, and failure summary.');
            console.log('');
            console.log('  analytics');
            console.log('    Calculate historical runtime metrics and throughput from run.log.');
            console.log('');
            console.log('  retry-failed');
            console.log('    Move all failed tweets to the END of the pending queue.');
            console.log('');
            console.log('  reset');
            console.log('    Archive all state files (requires confirmation).');
            console.log('    Does NOT delete anything — archives to state/archive_<ts>/');
            console.log('');
            process.exit(1);
    }
}

main().catch((err) => {
    console.error('Fatal error:', err.message);
    if (process.env.DEBUG) console.error(err.stack);
    process.exit(1);
});