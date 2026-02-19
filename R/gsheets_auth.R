# Auth helper for local interactive use and CI service-account use.
auth_google_sheets <- function(scopes = "https://www.googleapis.com/auth/spreadsheets") {
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required.")
  }

  sa_file <- Sys.getenv("GSHEETS_SERVICE_ACCOUNT_FILE", "")
  sa_json <- Sys.getenv("GSHEETS_SERVICE_ACCOUNT_JSON", "")

  if (nzchar(sa_file)) {
    if (!file.exists(sa_file)) {
      stop(sprintf("GSHEETS_SERVICE_ACCOUNT_FILE does not exist: %s", sa_file))
    }
    googlesheets4::gs4_auth(path = sa_file, scopes = scopes)
    return(invisible("service_account_file"))
  }

  if (nzchar(sa_json)) {
    tmp <- tempfile(fileext = ".json")
    writeLines(sa_json, con = tmp, useBytes = TRUE)
    on.exit(unlink(tmp), add = TRUE)
    googlesheets4::gs4_auth(path = tmp, scopes = scopes)
    return(invisible("service_account_json"))
  }

  # Fallback for local manual runs.
  googlesheets4::gs4_auth()
  invisible("interactive")
}
