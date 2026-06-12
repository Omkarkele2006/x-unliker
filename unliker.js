#!/usr/bin/env node
/**
 * x-unliker - script to unlike all tweets from your archive.
 * 
 * Usage:
 *   node unliker.js init --likes ./like.js --cookies ./cookies.json
 *   node unliker.js run
 *   node unliker.js status
 */

'use strict';

const fs = require('fs');
const path = require('path');
const https = require('https');
const crypto = require('crypto');
const os = require('os');
const readline = require('readline');

// Config
const CONFIG = {
    // Paths
    STATE_DIR: './state',
    PENDING_FILE: './state/pending.json',
    COMPLETED_FILE: './state/completed.ndjson',
    FAILED_FILE: './state/failed.json',
    CHECKPOINT_FILE: './state/checkpoint.json',
    COOKIES_FILE: './cookies.json',
    LOG_FILE: './state/run.log',

    // Rate limiting defaults (fallback if we don't get headers)
    RATE_LIMIT_WINDOW_MS: 15 * 60 * 1000,   // 900,000 ms
    DEFAULT_INTERVAL_MS: 4500,              // ~200 requests/window (40% of limit)
    MIN_INTERVAL_MS: 1000,              // don't go faster than 1 req/sec
    MAX_INTERVAL_MS: 60_000,            // wait at most 1 minute
    RATE_HEADROOM_REQUESTS: 20,                // buffer before hitting rate limit
    RATE_RESET_BUFFER_MS: 2000,              // safety buffer

    // Retry
    MAX_ATTEMPTS_PER_TWEET: 3,
    RETRY_BACKOFF_BASE_MS: 5000,
    RETRY_BACKOFF_MAX_MS: 60_000,
    RETRY_JITTER_MAX_MS: 2000,             // add some randomness

    // Stop if we get too many errors in a row
    CIRCUIT_BREAKER_THRESHOLD: 20,

    // Retries for 403 errors
    MAX_403_RETRIES: 4,
    BACKOFF_403_BASE_MS: 3000,

    // Checkpoint config
    CHECKPOINT_EVERY_N: 50,               // save checkpoint metadata every N unlikes

    // Batch writes
    COMPLETED_BATCH_SIZE: 25,               // flush completed entries every N
    COMPLETED_FLUSH_MS: 10_000,           // flush every 10s

    // X API settings
    GRAPHQL_HOST: 'x.com',
    GRAPHQL_PATH_TPL: '/i/api/graphql/{queryId}/UnfavoriteTweet',
    BEARER_TOKEN: null,                   // loaded from cookies
    QUERY_ID: null,                   // loaded from checkpoint

    // Safety limits
    DRY_RUN: false,
    MAX_PER_RUN: null,                        // null is unlimited
};

// Logging
let logStream = null;

function initLog() {
    fs.mkdirSync(CONFIG.STATE_DIR, { recursive: true });
    logStream = fs.createWriteStream(CONFIG.LOG_FILE, { flags: 'a' });
    logStream.on('error', (err) => {
        // don't crash on logging errors
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

// Buffer completed writes to avoid writing to disk every single time
const completedBuffer = [];
let completedFlushTimer = null;

function scheduleCompletedFlush() {
    if (completedFlushTimer) return;
    completedFlushTimer = setTimeout(() => {
        flushCompletedBuffer();
    }, CONFIG.COMPLETED_FLUSH_MS);
    completedFlushTimer.unref(); // let node exit even if timer is active
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
        // failed to write, put entries back to retry later
        logger.error('DISK WRITE FAILURE on completed.ndjson — entries buffered for retry', {
            error: err.message,
            code: err.code,
            buffered: completedBuffer.length + lines.split('\n').length - 1,
        });
        if (err.code === 'ENOSPC') {
            logger.error('DISK FULL. Free up space and restart — progress is safe.');
        }
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

// Load already completed IDs so we don't retry them
function loadCompletedSet() {
    const completed = new Set();
    if (!fs.existsSync(CONFIG.COMPLETED_FILE)) return completed;

    let raw;
    try {
        raw = fs.readFileSync(CONFIG.COMPLETED_FILE, 'utf8');
    } catch (err) {
        logger.warn('Could not read completed.ndjson — starting with empty set', { error: err.message });
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
            } else {
                malformed++;
            }
        } catch {
            malformed++;
        }
    }

    if (malformed > 0) {
        logger.warn('Malformed entries in completed.ndjson were skipped', { count: malformed });
    }

    logger.info('Loaded completed set', { count: completed.size, malformed });
    return completed;
}

// Write to temp file then rename to avoid corruption if it crashes
function atomicWriteJSON(filePath, data) {
    const tmp = filePath + '.tmp.' + process.pid;
    try {
        fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
        fs.renameSync(tmp, filePath);
    } catch (err) {
        // delete temp file if rename failed
        try { if (fs.existsSync(tmp)) fs.unlinkSync(tmp); } catch (_) { }
        if (err.code === 'ENOSPC') {
            logger.error('DISK FULL during atomic write — state NOT saved', {
                file: filePath,
                error: err.message,
            });
        } else {
            logger.error('Atomic write failed', { file: filePath, error: err.message });
        }
        throw err;
    }
}

function readJSONSafe(filePath, defaultValue) {
    try {
        if (!fs.existsSync(filePath)) return defaultValue;
        return JSON.parse(fs.readFileSync(filePath, 'utf8'));
    } catch (e) {
        logger.warn(`Failed to parse ${filePath}, using default`, { error: e.message });
        return defaultValue;
    }
}

// Expected cookies.json format:
// {
//   "bearerToken": "...",
//   "queryId": "...",
//   "cookies": { "auth_token": "...", "ct0": "..." }
// }
function protectCredentialsFile(filePath) {
    const platform = os.platform();
    if (platform === 'linux' || platform === 'darwin') {
        try {
            fs.chmodSync(filePath, 0o600);
            logger.info(`Credentials file permissions set to 600 (owner read/write only)`);
        } catch (err) {
            logger.warn('Could not chmod credentials file', { error: err.message });
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

// Parse like.js from Twitter archive
function parseLikeJs(filePath) {
    let content;
    try {
        content = fs.readFileSync(filePath, 'utf8');
    } catch (err) {
        throw new Error(`Cannot read like.js at ${filePath}: ${err.message}`);
    }

    const allIds = [];

    // Find each JSON array assigned to window.YTD.like.partN
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
            logger.warn('Failed to parse a like.js part — skipping', { error: err.message });
        }
    }

    // Fallback for older single-array files
    if (!matched || allIds.length === 0) {
        logger.warn('Multi-part regex found no parts — falling back to single-array parse');
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

    // Remove duplicate IDs
    const uniqueSet = new Set(allIds);
    const unique = [...uniqueSet];
    const dupeCount = allIds.length - unique.length;
    if (dupeCount > 0) {
        logger.info('Deduplicated like.js', { total: allIds.length, unique: unique.length, duplicates: dupeCount });
    } else {
        logger.info('Parsed like.js', { total: allIds.length, unique: unique.length });
    }

    return unique;
}

// Keep track of rate limits
const rateState = {
    limit: 500,
    remaining: 500,
    resetAt: Date.now() + 900_000,   // estimated; overwritten by first response

    // Parse headers from X
    update(headers) {
        const limit = headers['x-rate-limit-limit'];
        const remaining = headers['x-rate-limit-remaining'];
        const reset = headers['x-rate-limit-reset'];

        if (limit) this.limit = parseInt(limit, 10);
        if (remaining) this.remaining = parseInt(remaining, 10);
        if (reset) this.resetAt = parseInt(reset, 10) * 1000;
    },

    // Convert to JSON for saving
    toJSON() {
        return {
            limit: this.limit,
            remaining: this.remaining,
            resetAt: this.resetAt,
        };
    },

    // Reload saved rate state if it hasn't reset yet
    fromJSON(obj) {
        if (!obj) return;
        if (obj.resetAt && obj.resetAt > Date.now()) {
            this.limit = obj.limit ?? 500;
            this.remaining = obj.remaining ?? 500;
            this.resetAt = obj.resetAt;
            logger.info('Restored rate state from checkpoint', {
                remaining: this.remaining,
                resetIn: `${Math.round((this.resetAt - Date.now()) / 1000)}s`,
            });
        } else {
            logger.info('Checkpoint rate state is from a past window — using defaults');
        }
    },

    // Calculate delay based on remaining limit and window reset time
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

// API requests
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

    // Use user agent from cookies.json or fallback to a default
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

// Handle stale query ID
function isStaleQueryIdError(result) {
    if (result.status === 400 || result.status === 404) return true;
    if (result.errors) {
        for (const e of result.errors) {
            // 34 = not found, 214 = endpoint changed or needs flag
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

// Handle session expired
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

// Checkpoints
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
        rateState: null,   // saved rate limit state
    });
}

function saveCheckpoint(cp) {
    cp.updatedAt = new Date().toISOString();
    cp.rateState = rateState.toJSON();
    try {
        atomicWriteJSON(CONFIG.CHECKPOINT_FILE, cp);
    } catch (err) {
        logger.error('Failed to save checkpoint', { error: err.message });
        // not fatal, keep going
    }
}

// Utils
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

// safeWriteJSON
function safeWriteJSON(filePath, data, label) {
    try {
        atomicWriteJSON(filePath, data);
    } catch (err) {
        logger.error(`Failed to write ${label || filePath}`, { error: err.message });
    }
}

// Stop if too many errors in a row
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

// Main runner
async function runProcessor(creds) {
    // Load existing state
    const checkpoint = loadCheckpoint();
    const completedSet = loadCompletedSet();
    const failedMap = readJSONSafe(CONFIG.FAILED_FILE, {});
    const pending = readJSONSafe(CONFIG.PENDING_FILE, []);

    if (pending.length === 0) {
        logger.info('No pending tweets found. Run `node unliker.js status` to check state.');
        return;
    }

    // Restore query ID from checkpoint if needed
    if (checkpoint.queryId && checkpoint.queryId !== CONFIG.QUERY_ID) {
        logger.info('Restoring queryId from checkpoint', { queryId: checkpoint.queryId });
        CONFIG.QUERY_ID = checkpoint.queryId;
        creds.queryId = checkpoint.queryId;
    }

    // Restore rate limit state
    rateState.fromJSON(checkpoint.rateState);

    // Handle ctrl+c / shutdown
    let shutdownRequested = false;

    const handleSignal = (signal) => {
        logger.info(`${signal} received — finishing current request then saving state...`);
        shutdownRequested = true;
    };

    // only trigger shutdown once
    process.once('SIGINT', () => handleSignal('SIGINT'));
    process.once('SIGTERM', () => handleSignal('SIGTERM'));

    checkpoint.runCount = (checkpoint.runCount || 0) + 1;
    checkpoint.startedAt = checkpoint.startedAt ?? new Date().toISOString();

    logger.info('Starting run', {
        pending: pending.length,
        completed: completedSet.size,
        failed: Object.keys(failedMap).length,
        queryId: CONFIG.QUERY_ID,
        runCount: checkpoint.runCount,
        dryRun: CONFIG.DRY_RUN,
    });

    // Filter out tweets we already unliked
    const pendingArray = pending.filter((id) => !completedSet.has(id));
    logger.info('Effective pending after dedup against completed log', { count: pendingArray.length });

    let pendingIndex = 0;
    let sessionCount = 0;
    let staleQueryCount = 0;
    let consec403 = 0;   // consecutive 403s before giving up

    // Main loop
    while (pendingIndex < pendingArray.length) {

        // Check if user stopped it
        if (shutdownRequested) {
            logger.info('Shutdown requested — saving state and exiting cleanly.');
            break;
        }

        // Stop if too many errors
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

        // Double check we haven't unliked this already
        if (completedSet.has(tweetId)) {
            pendingIndex++;
            continue;
        }

        // Stop if we hit the limit for this run
        if (CONFIG.MAX_PER_RUN !== null && sessionCount >= CONFIG.MAX_PER_RUN) {
            logger.info(`Reached MAX_PER_RUN limit of ${CONFIG.MAX_PER_RUN}. Stopping cleanly.`);
            break;
        }

        // Skip if it failed too many times
        const failEntry = failedMap[tweetId];
        if (failEntry && failEntry.attempts >= CONFIG.MAX_ATTEMPTS_PER_TWEET) {
            logger.warn(`Skipping ${tweetId} — exceeded max attempts`, { attempts: failEntry.attempts });
            pendingIndex++;
            continue;
        }

        // Rate limit delay
        const waitMs = rateState.nextWaitMs();
        if (waitMs > 0) {
            if (waitMs >= 60_000) {
                const waitMin = (waitMs / 60_000).toFixed(1);
                const resetTime = new Date(rateState.resetAt).toISOString();
                logger.info(
                    `Rate limit reached — pausing ${waitMin} minutes until window reset at ${resetTime}`,
                    { waitMs, rateState: rateState.describe() }
                );
                // log status periodically so the user knows it's not frozen
                let slept = 0;
                while (slept < waitMs && !shutdownRequested) {
                    const chunk = Math.min(30_000, waitMs - slept);
                    await wait(chunk);
                    slept += chunk;
                    if (slept < waitMs && !shutdownRequested) {
                        const remaining = Math.round((waitMs - slept) / 1000);
                        logger.info(`Still paused — ${remaining}s until rate limit resets`);
                    }
                }
            } else if (waitMs >= 5_000) {
                logger.info(`Rate limit pause: ${Math.round(waitMs / 1000)}s — ${rateState.describe()}`);
                await wait(waitMs);
            } else {
                await wait(waitMs);
            }
        }

        // Re-check shutdown
        if (shutdownRequested) break;

        // Send request to X
        let result;
        try {
            result = await apiUnlikeTweet(tweetId, creds);
        } catch (networkErr) {
            logger.error(`Network error on ${tweetId}`, { error: networkErr.message });
            recordFailure(failedMap, tweetId, `NETWORK: ${networkErr.message}`);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            circuitBreaker.recordError();
            await wait(5000 + jitter(2000));
            // retry the same tweet
            continue;
        }

        // Update rate limits
        rateState.update(result.headers);

        // Handle response

        // Handle success
        if (result.ok) {
            completedSet.add(tweetId);
            queueCompleted({ id: tweetId, ts: Math.floor(Date.now() / 1000) });
            delete failedMap[tweetId];

            sessionCount++;
            pendingIndex++;
            staleQueryCount = 0;   // reset stale-query counter on success
            consec403 = 0;
            circuitBreaker.reset();

            checkpoint.completedCount = completedSet.size;
            checkpoint.lastTweetId = tweetId;
            checkpoint.queryId = CONFIG.QUERY_ID;

            if (sessionCount % CONFIG.CHECKPOINT_EVERY_N === 0) {
                // flush buffer and save checkpoint
                flushCompletedBuffer();
                saveCheckpoint(checkpoint);

                // trim pending.json every 500 unlikes to keep it small
                if (sessionCount % 500 === 0) {
                    const remaining = pendingArray.slice(pendingIndex);
                    safeWriteJSON(CONFIG.PENDING_FILE, remaining, 'pending.json');
                    logger.info('Checkpoint + pending file trimmed', {
                        session: sessionCount,
                        completed: completedSet.size,
                        remaining: remaining.length,
                        rate: rateState.describe(),
                    });
                } else {
                    logger.info('Checkpoint saved', {
                        session: sessionCount,
                        completed: completedSet.size,
                        rate: rateState.describe(),
                    });
                }
            }

            continue;
        }

        // Handle 429 rate limit
        if (result.status === 429) {
            const resetIn = Math.max(0, rateState.resetAt - Date.now() + CONFIG.RATE_RESET_BUFFER_MS);
            const resetMin = (resetIn / 60_000).toFixed(1);
            logger.warn(
                `429 Too Many Requests — waiting ${resetMin} minutes for rate limit reset`,
                { resetAt: new Date(rateState.resetAt).toISOString() }
            );
            // log wait progress
            let slept = 0;
            while (slept < resetIn && !shutdownRequested) {
                const chunk = Math.min(30_000, resetIn - slept);
                await wait(chunk);
                slept += chunk;
                if (slept < resetIn && !shutdownRequested) {
                    logger.info(`Rate limit wait: ${Math.round((resetIn - slept) / 1000)}s remaining`);
                }
            }
            // retry the same tweet
            continue;
        }

        // Handle 401 unauthorized
        if (result.status === 401) {
            flushCompletedBuffer();
            saveCheckpoint(checkpoint);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            printSessionExpiredInstructions();
            process.exit(1);
        }

        // 403 Forbidden - retry a few times before stopping
        if (result.status === 403) {
            consec403++;
            const backoffMs = Math.min(
                CONFIG.BACKOFF_403_BASE_MS * Math.pow(2, consec403 - 1),
                CONFIG.RETRY_BACKOFF_MAX_MS
            ) + jitter(CONFIG.RETRY_JITTER_MAX_MS);

            logger.warn(`403 Forbidden (attempt ${consec403}/${CONFIG.MAX_403_RETRIES})`, {
                body: result.raw.slice(0, 200),
                backoffMs,
            });

            if (consec403 < CONFIG.MAX_403_RETRIES) {
                // check if user updated cookies.json while running
                const freshCreds = readJSONSafe(CONFIG.COOKIES_FILE, null);
                if (freshCreds?.cookies?.ct0 && freshCreds.cookies.ct0 !== creds.cookies.ct0) {
                    logger.info('ct0 refreshed from cookies.json');
                    creds.cookies.ct0 = freshCreds.cookies.ct0;
                }
                await wait(backoffMs);
                continue; // retry same tweet
            }

            // too many 403 errors, exit
            logger.error(
                `403 persisted after ${CONFIG.MAX_403_RETRIES} retries. ` +
                'Session or CSRF token is invalid. Update cookies.json and restart.'
            );
            logger.error('Run: node unliker.js run   (after updating cookies.json)');
            flushCompletedBuffer();
            saveCheckpoint(checkpoint);
            safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
            process.exit(1);
        }

        // reset 403 counter
        consec403 = 0;

        // Handle stale query ID
        if (isStaleQueryIdError(result)) {
            staleQueryCount++;
            logger.warn(`Possible stale queryId on ${tweetId} (${staleQueryCount}/3)`, {
                status: result.status,
                errors: result.errors,
            });

            if (staleQueryCount >= 3) {
                logger.warn('Confirmed stale queryId after 3 consecutive errors.');
                const newId = await promptNewQueryId();
                if (newId) {
                    CONFIG.QUERY_ID = newId;
                    creds.queryId = newId;
                    const storedCreds = readJSONSafe(CONFIG.COOKIES_FILE, {});
                    storedCreds.queryId = newId;
                    safeWriteJSON(CONFIG.COOKIES_FILE, storedCreds, 'cookies.json');
                    checkpoint.queryId = newId;
                    saveCheckpoint(checkpoint);
                    staleQueryCount = 0;
                    circuitBreaker.reset();
                    logger.info('queryId updated. Resuming.');
                } else {
                    logger.error('No queryId provided. Saving state and stopping.');
                    flushCompletedBuffer();
                    saveCheckpoint(checkpoint);
                    safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
                    process.exit(1);
                }
            }
            // retry
            await wait(2000 + jitter(1000));
            continue;
        }

        // Handle already unliked or missing tweet
        if (result.status === 200 && !result.done) {
            logger.debug(`Tweet ${tweetId} returned 200/not-Done RAW=${result.raw}`);
            completedSet.add(tweetId);
            queueCompleted({ id: tweetId, ts: Math.floor(Date.now() / 1000), note: 'noop' });
            pendingIndex++;
            sessionCount++;
            staleQueryCount = 0;
            circuitBreaker.reset();
            checkpoint.completedCount = completedSet.size;
            continue;
        }

        // Handle other errors
        logger.warn(`Unexpected response for ${tweetId}`, {
            status: result.status,
            body: result.raw.slice(0, 300),
        });
        recordFailure(failedMap, tweetId, `HTTP_${result.status}: ${result.raw.slice(0, 100)}`);
        safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');
        circuitBreaker.recordError();
        pendingIndex++;
        sessionCount++;
        checkpoint.failedCount = Object.keys(failedMap).length;
    }

    // Save everything before exiting
    flushCompletedBuffer();
    const remaining = pendingArray.slice(pendingIndex);
    safeWriteJSON(CONFIG.PENDING_FILE, remaining, 'pending.json');
    saveCheckpoint(checkpoint);
    safeWriteJSON(CONFIG.FAILED_FILE, failedMap, 'failed.json');

    // clean up signal listeners
    process.removeAllListeners('SIGINT');
    process.removeAllListeners('SIGTERM');

    logger.info('Run complete', {
        sessionUnliked: sessionCount,
        totalCompleted: completedSet.size,
        remaining: remaining.length,
        failed: Object.keys(failedMap).length,
        circuitOpen: circuitBreaker.isOpen(),
        shutdownByUser: shutdownRequested,
    });

    if (remaining.length === 0 && Object.keys(failedMap).length === 0) {
        logger.info('All tweets unliked successfully.');
    } else if (remaining.length > 0) {
        logger.info('Run: node unliker.js run   to continue.');
    }
    if (Object.keys(failedMap).length > 0) {
        logger.info('Run: node unliker.js retry-failed   to retry errored tweets.');
    }
}

// Commands
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

    // Parse archive
    console.log('Parsing like.js...');
    const allIds = parseLikeJs(likesFile);
    console.log(`Found ${allIds.length} unique tweet IDs.`);

    // don't lose progress if re-initializing
    const completedSet = loadCompletedSet();
    const pending = allIds.filter((id) => !completedSet.has(id));
    console.log(`Already completed: ${completedSet.size}. Pending: ${pending.length}.`);

    // copy cookies if in a different path
    const canonicalCookies = path.resolve(CONFIG.COOKIES_FILE);
    if (path.resolve(cookiesFile) !== canonicalCookies) {
        fs.copyFileSync(cookiesFile, canonicalCookies);
        console.log(`Credentials copied to ${CONFIG.COOKIES_FILE}`);
    }

    // restrict permissions
    protectCredentialsFile(canonicalCookies);

    // test load cookies
    const creds = loadCredentials();
    console.log(`Credentials loaded. queryId: ${creds.queryId}`);

    // write initial pending queue
    atomicWriteJSON(CONFIG.PENDING_FILE, pending);
    console.log(`Wrote ${pending.length} IDs to ${CONFIG.PENDING_FILE}`);

    // set initial checkpoint
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

    // count what's left to unlike
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

    // calculate ETA based on speed of last 1000 unlikes
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

async function cmdRetryFailed() {
    const failedMap = readJSONSafe(CONFIG.FAILED_FILE, {});
    const pending = readJSONSafe(CONFIG.PENDING_FILE, []);

    const toRetry = Object.keys(failedMap);
    if (toRetry.length === 0) {
        console.log('No failed tweets to retry.');
        return;
    }

    // clear failure history and add back to the end of the queue
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

// Main
async function main() {
    const args = process.argv.slice(2);
    const cmd = args[0];

    switch (cmd) {
        case 'init': await cmdInit(args.slice(1)); break;
        case 'run': await cmdRun(args.slice(1)); break;
        case 'status': await cmdStatus(); break;
        case 'retry-failed': await cmdRetryFailed(); break;
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
