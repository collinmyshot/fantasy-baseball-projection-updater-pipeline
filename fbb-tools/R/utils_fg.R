# Shared FanGraphs fetch utility
#
# FanGraphs requires session cookies to bypass Cloudflare (added 2026).
# Cookie resolution order:
#   1. FG_COOKIE_PATH env var (set in .Renviron for local dev)
#   2. FG_COOKIE_CONTENT env var (shinyapps.io secret, if plan supports it)
#   3. fg_cookies.txt bundled with the app (gitignored, deployed manually)
#
# If fetches fail with "JSON parse failed — response may be a Cloudflare
# challenge", the session cookie has likely expired. Re-export from Chrome
# using "Get cookies.txt LOCALLY" while logged into fangraphs.com, replace
# fbb-tools/fg_cookies.txt, and redeploy.

FG_FETCH_AGENT <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

fg_resolve_cookie_path <- function() {
  path <- Sys.getenv("FG_COOKIE_PATH", unset = "")
  if (nzchar(path) && file.exists(path)) return(path)
  content <- Sys.getenv("FG_COOKIE_CONTENT", unset = "")
  if (nzchar(content)) {
    tmp <- tempfile(fileext = ".txt")
    writeLines(content, tmp)
    Sys.setenv(FG_COOKIE_PATH = tmp)
    return(tmp)
  }
  bundled <- "fg_cookies.txt"
  if (file.exists(bundled)) return(bundled)
  ""
}

# Fetch a FanGraphs API URL and return list(ok, payload) or list(ok=FALSE, error).
# referer should match the page the API is called from in a browser.
fg_fetch_json <- function(url, referer = "https://www.fangraphs.com") {
  if (!requireNamespace("curl", quietly = TRUE))
    return(list(ok = FALSE, error = "R package 'curl' is required"))

  cookie_path <- fg_resolve_cookie_path()

  h <- curl::new_handle()
  curl::handle_setheaders(h,
    "Accept"     = "application/json, text/plain, */*",
    "Referer"    = referer,
    "User-Agent" = FG_FETCH_AGENT
  )
  if (nzchar(cookie_path))
    curl::handle_setopt(h, cookiefile = cookie_path)

  resp <- tryCatch(
    curl::curl_fetch_memory(url, handle = h),
    error = function(e) list(status_code = 0L, content = NULL,
                             error = conditionMessage(e))
  )

  if (is.null(resp$content) || isTRUE(nzchar(resp[["error"]])))
    return(list(ok = FALSE, error = resp[["error"]] %||% "curl_fetch_memory failed"))
  if (resp$status_code != 200L)
    return(list(ok = FALSE, error = sprintf("HTTP %d from FanGraphs", resp$status_code)))

  payload <- tryCatch(
    jsonlite::fromJSON(rawToChar(resp$content), simplifyVector = TRUE),
    error = function(e) NULL
  )
  if (!is.null(payload)) return(list(ok = TRUE, payload = payload))
  list(ok = FALSE,
       error = "JSON parse failed — response may be a Cloudflare challenge (check fg_cookies.txt)")
}
