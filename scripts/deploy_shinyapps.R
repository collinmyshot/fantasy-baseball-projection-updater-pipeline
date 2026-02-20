#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(rsconnect)
})

ensure_valid_repos <- function() {
  repos <- getOption("repos")
  cran <- suppressWarnings(as.character(repos[["CRAN"]]))
  bad <- length(cran) == 0 || is.na(cran) || !nzchar(cran) || cran %in% c("@CRAN@", "CRAN")
  if (bad) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
  }
  invisible(getOption("repos"))
}

normalize_secret <- function(x) {
  x <- trimws(as.character(x))
  x <- sub('^"(.*)"$', "\\1", x)
  x <- sub("^'(.*)'$", "\\1", x)
  x
}

args <- commandArgs(trailingOnly = TRUE)
app_dir <- if (length(args) >= 1) args[[1]] else "."
app_name <- if (length(args) >= 2) args[[2]] else Sys.getenv("SHINYAPP_NAME", "fantasy-baseball-projections")

account <- normalize_secret(Sys.getenv("SHINYAPPS_ACCOUNT", ""))
token <- normalize_secret(Sys.getenv("SHINYAPPS_TOKEN", ""))
secret <- normalize_secret(Sys.getenv("SHINYAPPS_SECRET", ""))

if (!nzchar(account) || !nzchar(token) || !nzchar(secret)) {
  stop(
    paste(
      "Missing deploy credentials. Set:",
      "SHINYAPPS_ACCOUNT, SHINYAPPS_TOKEN, SHINYAPPS_SECRET",
      sep = "\n"
    ),
    call. = FALSE
  )
}

ensure_valid_repos()

message("Deploying app '", app_name, "' from ", normalizePath(app_dir, mustWork = FALSE))

rsconnect::setAccountInfo(
  name = account,
  token = token,
  secret = secret,
  server = "shinyapps.io"
)

rsconnect::deployApp(
  appDir = app_dir,
  appName = app_name,
  account = account,
  server = "shinyapps.io",
  launch.browser = FALSE,
  forceUpdate = TRUE
)
