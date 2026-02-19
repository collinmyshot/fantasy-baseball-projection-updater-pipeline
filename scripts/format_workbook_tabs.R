#!/usr/bin/env Rscript

args_raw <- commandArgs(trailingOnly = TRUE)

config_path <- file.path("config", "pipeline.yml")
args <- character(0)
i <- 1
while (i <= length(args_raw)) {
  arg <- args_raw[[i]]
  if (arg == "--config" && i < length(args_raw)) {
    config_path <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--config=")) {
    config_path <- sub("^--config=", "", arg)
    i <- i + 1
    next
  }
  args <- c(args, arg)
  i <- i + 1
}

source(file.path("R", "pipeline_config.R"))
source(file.path("R", "gsheets_auth.R"))
cfg <- load_pipeline_config(config_path)

target_sheet <- if (length(args) >= 1) args[[1]] else cfg$google_sheets$workbook_url

if (!nzchar(target_sheet)) {
  stop("Usage: Rscript scripts/format_workbook_tabs.R <target_sheet_url> [--config path]")
}

if (!requireNamespace("googlesheets4", quietly = TRUE)) {
  stop("Package 'googlesheets4' is required.")
}

safe_header <- function(ss, sheet_name) {
  out <- tryCatch(
    googlesheets4::read_sheet(
      ss,
      sheet = sheet_name,
      range = "1:1",
      col_names = FALSE,
      .name_repair = "minimal"
    ),
    error = function(e) NULL
  )
  if (is.null(out) || nrow(out) == 0) {
    return(character(0))
  }
  vals <- as.character(unlist(out[1, ], use.names = FALSE))
  vals[is.na(vals)] <- ""
  vals
}

derive_freeze_cols <- function(sheet_name, header_vals, n_cols) {
  # Fast path for known tabs to reduce read/load churn.
  projection_tab <- cfg$google_sheets$projection_tab
  adp_tab <- cfg$google_sheets$adp_tab
  rbll_tab <- cfg$google_sheets$rbll_tab
  run_data_tab <- cfg$google_sheets$run_data_tab
  known <- NULL
  if (identical(sheet_name, run_data_tab)) {
    known <- 1L
  } else if (identical(sheet_name, projection_tab)) {
    known <- 5L
  } else if (identical(sheet_name, adp_tab)) {
    known <- 3L
  } else if (identical(sheet_name, rbll_tab)) {
    known <- 4L
  } else if (sheet_name %in% c("C", "1B", "2B", "3B", "SS", "OF")) {
    known <- 4L
  }
  if (!is.null(known)) {
    return(max(1L, min(as.integer(n_cols), known)))
  }

  hdr_up <- toupper(trimws(header_vals))
  pa_idx <- which(hdr_up == "PA")
  adp_idx <- which(hdr_up == "ADP")

  freeze_cols <- if (length(pa_idx) > 0) {
    pa_idx[1]
  } else if (length(adp_idx) > 0) {
    adp_idx[1]
  } else {
    1L
  }
  max(1L, min(as.integer(n_cols), as.integer(freeze_cols)))
}

message("Authenticating Google Sheets...")
auth_google_sheets()

ss_id <- as.character(unclass(googlesheets4::as_sheets_id(target_sheet)))
props <- googlesheets4::sheet_properties(target_sheet)

requests <- list()

for (i in seq_len(nrow(props))) {
  sheet_name <- props$name[i]
  sheet_id <- props$id[i]
  n_rows <- as.integer(props$grid_rows[i])
  n_cols <- as.integer(props$grid_columns[i])

  known_tabs <- c(cfg$google_sheets$run_data_tab, cfg$google_sheets$projection_tab, cfg$google_sheets$adp_tab, cfg$google_sheets$rbll_tab, "C", "1B", "2B", "3B", "SS", "OF")
  hdr <- if (sheet_name %in% known_tabs) character(0) else safe_header(target_sheet, sheet_name)
  freeze_cols <- derive_freeze_cols(sheet_name, hdr, n_cols)

  requests[[length(requests) + 1L]] <- list(
    updateSheetProperties = list(
      properties = list(
        sheetId = sheet_id,
        gridProperties = list(
          frozenRowCount = 1L,
          frozenColumnCount = freeze_cols
        )
      ),
      fields = "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
    )
  )

  requests[[length(requests) + 1L]] <- list(
    setBasicFilter = list(
      filter = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 0L,
          endRowIndex = n_rows,
          startColumnIndex = 0L,
          endColumnIndex = n_cols
        )
      )
    )
  )

  requests[[length(requests) + 1L]] <- list(
    repeatCell = list(
      range = list(
        sheetId = sheet_id,
        startRowIndex = 0L,
        endRowIndex = n_rows,
        startColumnIndex = 0L,
        endColumnIndex = n_cols
      ),
      cell = list(
        userEnteredFormat = list(
          horizontalAlignment = "CENTER",
          verticalAlignment = "MIDDLE"
        )
      ),
      fields = "userEnteredFormat(horizontalAlignment,verticalAlignment)"
    )
  )

  requests[[length(requests) + 1L]] <- list(
    repeatCell = list(
      range = list(
        sheetId = sheet_id,
        startRowIndex = 0L,
        endRowIndex = 1L,
        startColumnIndex = 0L,
        endColumnIndex = n_cols
      ),
      cell = list(
        userEnteredFormat = list(
          textFormat = list(bold = TRUE),
          horizontalAlignment = "CENTER",
          verticalAlignment = "MIDDLE"
        )
      ),
      fields = "userEnteredFormat(textFormat.bold,horizontalAlignment,verticalAlignment)"
    )
  )
}

req <- googlesheets4::request_generate(
  "sheets.spreadsheets.batchUpdate",
  params = list(
    spreadsheetId = ss_id,
    requests = requests
  )
)

googlesheets4::request_make(req)
message("Workbook formatting applied to all sheets.")
