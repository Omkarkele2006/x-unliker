# x-unliker

A small Node.js utility for removing likes from X/Twitter using your own archive export.

Built after discovering I had more than 130,000 liked tweets.

---

## Why this exists

I accumulated years of likes on Twitter and wanted to clean up my digital footprint. I quickly found out that doing this is surprisingly hard:
- Third-party deletion services either require full OAuth write-access to your account (which is a massive security risk) or charge ridiculous fees.
- The official Twitter developer API is locked behind expensive tiers or has extremely low free limits.
- The web interface only lets you click "Unlike" one by one, and it gets sluggish after scrolling down a few pages.

This tool solves that. It runs completely locally on your computer, parsing your official Twitter archive download to get your liked tweet list, and then replays the unfavorite requests using the exact endpoint that the X web app uses when you click "unlike" in your browser. 

I wrote this utility to be practical. It keeps track of progress, respects rate limits and can resume after interruptions, which is useful when you're processing thousands of likes.

---

## Real-World Run Results

This project was originally built to clean up my own X/Twitter account after discovering I had accumulated more than 133,000 liked tweets over the years.

Final run statistics:

- Tweets processed: 133,863
- Run sessions: 20
- Completion rate: 100%
- Remaining failures: 0
- Observed throughput: ~1,900 tweets/hour
- Project duration: June 12–17, 2026

The tool was able to resume across multiple sessions, survive network interruptions, recover from expired credentials, and eventually process the entire archive without data loss.

---

## Features

- **Local & Private:** No database, no web servers, and no analytics. Your credentials never leave your machine.
- **Archive Parsing:** Reads your official Twitter archive export files directly, automatically deduplicating tweet IDs and handling multi-part files.
- **Resilient Queue Management:** Saves state locally in JSON and line-delimited JSON formats so you can close the terminal, resume after crashes or restarts, and pick up right where you left off.
- **Automatic Retry Handling:** Retries failed requests with exponential backoff and jitter for transient network or session issues.
- **Rate Limit Aware:** Monitors headers returned by X (`x-rate-limit-remaining` and `x-rate-limit-reset`) to calculate optimal wait windows dynamically.
- **Circuit Breaker:** Halts execution automatically if it detects systematic failures (like continuous network issues or a revoked browser session) to prevent spamming endpoints.
- **Interactive Stale Query Handling:** If X deploys an update and changes their internal GraphQL Query ID, the script detects it, tells you how to get the new one, and prompts you to paste it in to continue without having to modify any source code.
- **Runtime Analytics and Reporting:** Built-in reporting command to analyze log history and output exact runtime execution spans, throughput rates, and pauses.
- **Dry-Run Mode:** Test your setup first to verify cookies and parse your archive without sending any actual write requests.

---

## How it works

The script operates in three main steps:
1. **Parsing:** During initialization, the script scans your exported `like.js` file for tweet IDs. It dedupes the list and compares it against your completed log from previous runs to build a list of tweets that still need to be unliked.
2. **Rate Limit Management:** X typically limits GraphQL unfavorite actions to 500 requests per 15 minutes. The script calculates a delay between requests to spread your remaining limit across the active window. If the limit is reached, it enters a countdown state.
3. **Execution:** The script sends standard HTTPS requests to the GraphQL endpoint (`/i/api/graphql/{queryId}/UnfavoriteTweet`) with your specific authorization headers, cookies, and user agent. When a tweet is unliked, it writes the ID to `state/completed.ndjson` and updates `state/checkpoint.json` as a safeguard.

---

## Security Warning

> [!CAUTION]
> **This script requires your raw session tokens (`auth_token` and `ct0`) to function.**
> These tokens are equivalent to your account password. Anyone who gets hold of them can log into your account and perform any action on your behalf.
> - **Never** commit your `cookies.json` to GitHub. The included `.gitignore` is configured to ignore it by default.
> - **Never** upload your configuration or logs to public sites.
> - Store the project folder in a secure directory on your local drive.
> - When you are finished unliker runs, it is a good idea to log out of the browser session you copied the cookies from, which immediately invalidates the tokens.

---

## Prerequisites

- **Node.js** (v16.0.0 or higher). No external npm packages are needed; it uses only built-in Node modules.
- **Your Twitter Data Archive:** You must request your data export from X (**Settings -> Your Account -> Download an archive of your data**). It usually takes 24–72 hours for X to generate the zip file.
- **Session cookies:** A browser where you are logged into your X account.

---

## Setup

### 1. Clone the project and prepare the config file
Run the following commands in your terminal:
```bash
git clone https://github.com/Omkarkele2006/x-unliker.git
cd x-unliker
cp cookies.example.json cookies.json
```

### 2. Extract your Twitter archive
Once your data download is ready from Twitter:
1. Extract the downloaded zip file.
2. Locate the file `like.js` inside the `data/` subdirectory.
3. Copy `like.js` directly into your `x-unliker` directory.

---

## Extracting Credentials from your Browser

To authenticate the script, you need to copy active browser session keys into `cookies.json`.

1. Open your browser (Google Chrome or Mozilla Firefox work best) and go to [x.com](https://x.com). Make sure you are logged in.
2. Open **Developer Tools** by pressing `F12` (or right-click anywhere and select **Inspect**), then click the **Network** tab.
3. In the filter box in the top-left of the Network tab, type `graphql`.
4. Now, go to any tweet on your timeline or profile and click the **Like** button, then click it again to **Unlike** it.
5. In the Network tab, you will see a request appear named `UnfavoriteTweet` (or sometimes `FavoriteTweet` first). Click on the **UnfavoriteTweet** entry.
6. Look at the request details panel:
   - **Find the Query ID:** Look at the request URL. It looks like `https://x.com/i/api/graphql/XYZ123_abc/UnfavoriteTweet`. Copy the random string between `graphql/` and `/UnfavoriteTweet` (in this case, `XYZ123_abc`). Open `cookies.json` and paste it as the value for `"queryId"`.
   - **Find the Bearer Token:** Look at the **Request Headers** section. Find the `authorization` header. It starts with `Bearer `. Copy the entire long string after `Bearer ` and paste it into `"bearerToken"` in your `cookies.json`.
7. Extract the Cookie values:
   - In Google Chrome: Go to the **Application** tab in DevTools, expand the **Cookies** menu on the left side, and select `https://x.com`.
   - In Mozilla Firefox: Go to the **Storage** tab in DevTools, expand the **Cookies** menu, and select `https://x.com`.
   - Locate the cookie named `auth_token`. Copy its value and paste it into `"cookies"` -> `"auth_token"` in your `cookies.json`.
   - Locate the cookie named `ct0`. Copy its value and paste it into `"cookies"` -> `"ct0"` in your `cookies.json`.
   - Locate your browser's user agent under request headers (or by typing `navigator.userAgent` in the DevTools console tab). Paste it into the `"userAgent"` field of `cookies.json`.

Save your changes. Your `cookies.json` should have this layout:
```json
{
  "bearerToken": "AAAAAAAAAAAAAAAAAAAAANRILgAAAAA...",
  "queryId": "ZYKSe-w7KEslx3J.....",
  "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ...",
  "cookies": {
    "auth_token": "789fe164fb468ead60148f1202431......",
    "ct0": "63e9dc4afce75c8eedb44628d23e06eaa....."
  }
}
```

---

## Usage Guide

### 1. Initialize State
Before running the main loops, process your archive to set up local queue tracking:
```bash
node unliker.js init --likes ./like.js --cookies ./cookies.json
```
This script reads your `like.js`, filters out any tweets you previously marked as completed, and writes a pending queue list to `state/pending.json`.

### 2. Verify with a Dry Run
It's a good practice to test your credentials first. Running this command will parse your cookies, load your queue, and pretend to unlike tweets without sending actual network requests:
```bash
node unliker.js run --dry-run
```

### 3. Start Unliking
If the dry run finishes without throwing errors, start the real process:
```bash
node unliker.js run
```
To stop the script at any time, press `Ctrl+C`. The script will complete the active tweet request, flush its memory buffers to the logs, and exit cleanly without corrupting the files.

### 4. Monitor Progress
You can check how many tweets are left and see an ETA based on your average processing speed by opening a new terminal window and running:
```bash
node unliker.js status
```

### 5. Runtime Analytics
To view advanced execution statistics across all completed runs, including active processing time vs. idle/sleep gaps and rate limit pauses:
```bash
node unliker.js analytics
```

The analytics command reports:
- **Project Span & Duration**: Total timeline since the first run.
- **Session Statistics**: Number of separate runs and total active execution time.
- **Rate-Limit Pauses**: Total occurrences and duration spent waiting on rate limit windows.
- **Throughput & Efficiency**: Real-world and active processing rates, along with average request intervals.

---

## Commands Reference

- `init --likes <path> --cookies <path>`: Scans the specified `like.js` file, filters out completed items, copies the cookies to the workspace, and initializes the queue database in `state/`.
- `run [options]`: Starts unliking tweets.
  - `--dry-run`: Performs a simulated execution path.
  - `--max <number>`: Stops execution after successfully unliking the specified number of tweets. Handy for testing runs in smaller batches.
- `status`: Reads log history and gives progress stats, remaining counts, and ETAs.
- `analytics`: Reads `state/run.log` and `state/completed.ndjson` to calculate runtime metrics, active session times, rate limit pauses, and throughput.
- `retry-failed`: If some tweets continuously throw errors (like a deleted tweet or bad connection), they are flagged as failed. Run this command to move them back to the end of the queue for another try.
- `reset`: Archives all run history, logs, and checkpoints into a backup subdirectory under `state/` so you can re-run initialization from scratch.

---

## Recovering from Interruptions

- **Rate Limits (HTTP 429):** The script parses rate limit headers and adjusts sleeps. If you hit a hard block, it will print a warning and sleep until the reset timestamp (usually up to 15 minutes) before resuming.
- **Expired Sessions (HTTP 401 / 403):** If your browser session is terminated or logs out, the script will dump state to files and exit. Grab new cookie values using the steps in the credentials guide, update `cookies.json`, and run the script again.
- **Stale Query IDs (HTTP 400 / 404):** If Twitter deploys an update, your query ID might become stale. The script will notice this, present clear instructions on how to find the updated query ID, and prompt you to input it directly in your terminal. It will save the new ID to your configuration automatically.
- **Network Outages:** If your Wi-Fi drops, the script pauses, sleeps with exponential backoff, and retries the request once connection is restored.

---

## Architecture

```
like.js
  ↓
pending queue (state/pending.json)
  ↓
GraphQL requests (/i/api/graphql/.../UnfavoriteTweet)
  ↓
completed log (state/completed.ndjson)
  ↓
analytics (command)
```

The script parses `like.js` to build a local list of pending tweet IDs. It processes them sequentially using Twitter's internal GraphQL endpoint. Successful requests are logged immediately to a line-delimited JSON file, which serves as the data source for progress tracking and runtime analytics.

---

## Disclaimer

This is a personal utility developed to help individuals clean up their personal accounts. It is not affiliated with, sponsored by, or endorsed by Twitter/X. Using automated scripts to perform actions on X can sometimes violate their Terms of Service. Use this tool at your own discretion. The author is not responsible for any actions taken by X against your account, including suspensions or restrictions.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
