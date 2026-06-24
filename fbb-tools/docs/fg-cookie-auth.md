# FanGraphs API Cookie-Auth Integration

A reference for finding, understanding, and maintaining the authenticated
FanGraphs API integration in the fbb-tools Shiny app.

## What this is

FanGraphs added Cloudflare bot protection to their public API in **early 2026**,
which broke every direct `jsonlite::fromJSON()` / `system2(curl, ...)` call in the
app — they started returning a Cloudflare challenge HTML page instead of JSON.

The fix routes **all** FanGraphs API calls through one shared helper that attaches
an authenticated session cookie (from a paid FanGraphs membership) so the request
clears Cloudflare.

## Two repos (important)

| Role | Repo | Where the Shiny code lives |
|------|------|----------------------------|
| **Dev** | `collinmyshot/fantasy-baseball-projection-updater-pipeline` | `fbb-tools/` subdir on branch **`claude/frosty-moore`** (not `main`) |
| **Prod** | `collinmyshot/fbb-tools` | repo **root** (same files, no `fbb-tools/` prefix) |

Both repos carry the same code and the same integration. File paths below assume
the **dev** repo; for prod, drop the `fbb-tools/` prefix.

## How to find it in git

```bash
git checkout claude/frosty-moore
git log --all --oneline -- fbb-tools/R/utils_fg.R
grep -rln "fg_fetch_json\|utils_fg" fbb-tools/
```

Key commits (dev repo):
- `1e0363f` — Fix RP Skillz FanGraphs fetch to bypass Cloudflare with session cookies (initial)
- `4929c65` — Add bundled cookie fallback and gitignore fg_cookies.txt
- `f873357` — Migrate rp_skillz + sp_skillz FG fetch to shared utils_fg cookie auth (extraction to shared utility)
- `dd192d1` — Document FanGraphs cookie auth setup in CLAUDE.md

## Architecture — three files matter

### 1. `fbb-tools/R/utils_fg.R` — the shared fetch function
- Exports `fg_fetch_json(url, referer)` and `fg_resolve_cookie_path()`.
- Uses the **R `curl` package** (`curl::curl_fetch_memory()` + `curl::handle_setopt(cookiefile = ...)`),
  **NOT** `system2(curl, ...)`. The old `system2` approach broke on Windows because the
  spaces/parens in the User-Agent string were mis-quoted, so curl treated each word as a
  separate URL.
- Returns `list(ok = TRUE/FALSE, payload = ..., error = ...)`.
- Sourced near the top of `fbb-tools/app.R`.

### 2. `fbb-tools/fg_cookies.txt` — the actual cookies (**GITIGNORED**)
- Netscape-format cookie file (the format `curl -b` reads).
- Exported from Chrome with the **"Get cookies.txt LOCALLY"** extension while logged
  into fangraphs.com.
- **Not in any git history — don't search git for it.** It is listed in `.gitignore`.
- Local source on the user's machine: `C:\Users\Collin\Downloads\www.fangraphs.com_cookies.txt`.
- Ships to shinyapps.io inside the `rsconnect::deployApp` bundle (that's how the
  deployed app gets credentials despite the file never being committed).

### 3. `fbb-tools/CLAUDE.md` (or root `CLAUDE.md` in prod)
- "FanGraphs Cookie Authentication" section has the authoritative setup notes.
- `grep -A 20 "FanGraphs Cookie" CLAUDE.md`

## Cookie resolution order (`fg_resolve_cookie_path()`)

1. `Sys.getenv("FG_COOKIE_PATH")` — set via a gitignored `.Renviron` for local dev.
2. `Sys.getenv("FG_COOKIE_CONTENT")` — raw cookie text as a secret env var (for
   shinyapps.io paid tier; **not currently used** — the account is on the free tier,
   which has no env-var support, so the bundled file is the live mechanism).
3. Bundled fallback: `fg_cookies.txt` in the working directory.

## Modules that use it (8)

`mod_rp_skillz.R`, `mod_sp_skillz.R`, `mod_proj_agg.R`, `mod_auc_val.R`,
`mod_hit_dashboard.R`, `mod_pit_dashboard.R`, `mod_team_rater.R`, `mod_sp_streamonator.R`

Common pattern:

```r
some_fetch_function <- function(url) {
  fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
}
```

Different endpoints need different `Referer` headers:
- Leaders / dashboards: `https://www.fangraphs.com/leaders/major-league`
- Projections (`mod_proj_agg.R`): `https://www.fangraphs.com/projections`
- Probables grid (`mod_sp_streamonator.R`): `https://www.fangraphs.com/roster-resource/probables-grid`

## Verify it's working

From `fbb-tools/`:

```r
Sys.setenv(FG_COOKIE_PATH = "C:/Users/Collin/Downloads/www.fangraphs.com_cookies.txt")
source("R/utils_fg.R")
res <- fg_fetch_json(
  paste0("https://www.fangraphs.com/api/leaders/major-league/data",
         "?pos=all&stats=pit&lg=all&ind=0&qual=0&type=3",
         "&season=2026&season1=2026&month=0&pageitems=2000&pagenum=1"),
  referer = "https://www.fangraphs.com/leaders/major-league"
)
res$ok   # TRUE = cookies working
```

## When it breaks: expired session cookie

Symptom — fetches return:

```
JSON parse failed — response may be a Cloudflare challenge (check fg_cookies.txt)
```

Most likely the session cookie expired (the original export was dated ~2027, so this
shouldn't happen until then, but a logout/password change invalidates it sooner).

Fix:
1. Log into fangraphs.com in Chrome.
2. Re-export cookies with the "Get cookies.txt LOCALLY" extension.
3. Replace `fbb-tools/fg_cookies.txt` with the new file.
4. Redeploy: `rsconnect::deployApp(appDir='.', appName='fbb-tools', account='collinmyshot', forceUpdate=TRUE)` from `fbb-tools/`.

## Ethical / business note

The user is a paying FanGraphs member and respects FanGraphs as a business. Using a
single member's cookie to serve all visitors of the public Shiny app is a known
trade-off they accepted **temporarily** to get the app working. A per-user cookie
upload flow (each visitor supplies their own FanGraphs cookies) was discussed as the
proper long-term fix and is **not yet implemented**. Keep this in mind before
expanding usage.
