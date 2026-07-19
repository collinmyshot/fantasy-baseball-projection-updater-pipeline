# Park factor Google Sheets formatting utilities.
# Extracted from scripts/park_factors/build_park_factor_clean_2026.R for reuse.

if (!exists("frames_equal")) source(file.path("R", "utils.R"))

apply_pf_sheet_format <- function(sheet_url, tab_name, dat, freeze_header = TRUE, known_mode = FALSE) {
  props <- googlesheets4::sheet_properties(sheet_url)
  if (is.null(props) || nrow(props) == 0 || !tab_name %in% props$name) {
    return(invisible(FALSE))
  }
  row_idx <- match(tab_name, props$name)
  sheet_id <- as.integer(props$id[row_idx])
  ss_id <- as.character(unclass(googlesheets4::as_sheets_id(sheet_url)))

  used_rows <- as.integer(max(1L, nrow(dat) + 1L))
  used_cols <- as.integer(max(1L, ncol(dat)))
  index_cols <- which(
    grepl("_IDX_100$", names(dat)) |
      names(dat) %in% c(
        "Overall Park Factor", "BACON Park Factor", "HR Park Factor",
        "Overall.Park.Factor", "BACON.Park.Factor", "HR.Park.Factor"
      )
  )
  rank_col <- match("Rank", names(dat))
  total_bbe_col <- match("Total BBE", names(dat))
  if (is.na(total_bbe_col)) total_bbe_col <- match("Total_BBE", names(dat))
  if (is.na(total_bbe_col)) total_bbe_col <- match("Totall BBE", names(dat))
  if (is.na(total_bbe_col)) total_bbe_col <- match("Total.BBE", names(dat))
  text_cols <- which(names(dat) %in% c("Team", "Park", "Years", "Analysis", "Notes"))
  notes_col <- match("Notes", names(dat))
  team_col <- match("Team", names(dat))

  get_conditional_rule_count <- function() {
    tryCatch(
      {
        if (!requireNamespace("jsonlite", quietly = TRUE)) {
          return(0L)
        }
        meta_req <- googlesheets4::request_generate(
          "sheets.spreadsheets.get",
          params = list(
            spreadsheetId = ss_id,
            fields = "sheets(properties(sheetId,title),conditionalFormats)"
          )
        )
        meta_resp <- googlesheets4::request_make(meta_req)
        meta <- jsonlite::fromJSON(rawToChar(meta_resp$content), simplifyVector = FALSE)
        if (is.null(meta$sheets)) {
          return(0L)
        }
        hit <- which(vapply(meta$sheets, function(s) {
          sid <- suppressWarnings(as.integer(s$properties$sheetId))
          !is.na(sid) && sid == sheet_id
        }, logical(1)))
        if (length(hit) == 0) {
          return(0L)
        }
        cf <- meta$sheets[[hit[1]]]$conditionalFormats
        if (is.null(cf)) 0L else as.integer(length(cf))
      },
      error = function(e) 0L
    )
  }

  existing_rule_count <- get_conditional_rule_count()

  requests <- list()

  if (existing_rule_count > 0L) {
    for (idx in seq(existing_rule_count - 1L, 0L, by = -1L)) {
      requests[[length(requests) + 1L]] <- list(
        deleteConditionalFormatRule = list(
          sheetId = sheet_id,
          index = as.integer(idx)
        )
      )
    }
  }

  requests <- c(
    requests,
    list(
    list(
      updateSheetProperties = list(
        properties = list(
          sheetId = sheet_id,
          gridProperties = list(
            frozenRowCount = if (isTRUE(freeze_header)) 1L else 0L,
            frozenColumnCount = 0L
          )
        ),
        fields = "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"
      )
    ),
    if (!isTRUE(known_mode)) list(
      setBasicFilter = list(
        filter = list(
          range = list(
            sheetId = sheet_id,
            startRowIndex = 0L,
            endRowIndex = used_rows,
            startColumnIndex = 0L,
            endColumnIndex = used_cols
          )
        )
      )
    ) else NULL,
    list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 0L,
          endRowIndex = used_rows,
          startColumnIndex = 0L,
          endColumnIndex = used_cols
        ),
        cell = list(
          userEnteredFormat = list(
            horizontalAlignment = "CENTER",
            verticalAlignment = "MIDDLE"
          )
        ),
        fields = "userEnteredFormat(horizontalAlignment,verticalAlignment)"
      )
    ),
    list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 0L,
          endRowIndex = 1L,
          startColumnIndex = 0L,
          endColumnIndex = used_cols
        ),
        cell = list(
          userEnteredFormat = list(
            textFormat = list(bold = TRUE),
            backgroundColor = list(red = 0.91, green = 0.95, blue = 0.98),
            horizontalAlignment = "CENTER",
            verticalAlignment = "MIDDLE"
          )
        ),
        fields = "userEnteredFormat(textFormat.bold,backgroundColor,horizontalAlignment,verticalAlignment)"
      )
    ),
    list(
      autoResizeDimensions = list(
        dimensions = list(
          sheetId = sheet_id,
          dimension = "COLUMNS",
          startIndex = 0L,
          endIndex = used_cols
        )
      )
    ),
    if (!isTRUE(known_mode)) list(
      updateDimensionProperties = list(
        range = list(
          sheetId = sheet_id,
          dimension = "COLUMNS",
          startIndex = 4L,
          endIndex = 10L
        ),
        properties = list(
          pixelSize = 130L
        ),
        fields = "pixelSize"
      )
    ) else NULL
  ))
  requests <- Filter(Negate(is.null), requests)

  if (isTRUE(known_mode) && used_cols >= 10L) {
    set_width <- function(start_idx, end_idx, px) {
      requests[[length(requests) + 1L]] <<- list(
        updateDimensionProperties = list(
          range = list(
            sheetId = sheet_id,
            dimension = "COLUMNS",
            startIndex = as.integer(start_idx),
            endIndex = as.integer(end_idx)
          ),
          properties = list(pixelSize = as.integer(px)),
          fields = "pixelSize"
        )
      )
    }
    set_width(0L, 1L, 55L)   # Rank
    set_width(1L, 2L, 110L)  # Team
    set_width(2L, 3L, 155L)  # Park
    set_width(3L, 4L, 85L)   # Years
    set_width(4L, 6L, 95L)   # BACON/HR or 1H/2H PF
    set_width(6L, 8L, 90L)   # Difference / Abs Difference
    set_width(8L, 9L, 80L)   # Total BBE
    set_width(9L, 10L, 165L) # Notes
  }

  if (length(index_cols) > 0) {
    for (k in seq_along(index_cols)) {
      col_idx <- as.integer(index_cols[[k]])
      col_vals <- suppressWarnings(as.numeric(dat[[col_idx]]))
      col_vals <- col_vals[is.finite(col_vals)]
      midpoint_value <- if (length(col_vals) > 0) stats::median(col_vals, na.rm = TRUE) else 100
      rng <- list(
        sheetId = sheet_id,
        startRowIndex = 1L,
        endRowIndex = used_rows,
        startColumnIndex = as.integer(col_idx - 1L),
        endColumnIndex = as.integer(col_idx)
      )

      requests[[length(requests) + 1L]] <- list(
        addConditionalFormatRule = list(
          index = 0L,
          rule = list(
            ranges = list(rng),
            gradientRule = list(
              minpoint = list(
                type = "MIN",
                color = list(red = 0.33, green = 0.54, blue = 0.96)
              ),
              midpoint = list(
                type = "NUMBER",
                value = sprintf("%.6f", as.numeric(midpoint_value)),
                color = list(red = 1.00, green = 1.00, blue = 1.00)
              ),
              maxpoint = list(
                type = "MAX",
                color = list(red = 0.93, green = 0.33, blue = 0.31)
              )
            )
          )
        )
      )
    }
  }

  if (!is.na(rank_col)) {
    requests[[length(requests) + 1L]] <- list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 1L,
          endRowIndex = used_rows,
          startColumnIndex = as.integer(rank_col - 1L),
          endColumnIndex = as.integer(rank_col)
        ),
        cell = list(
          userEnteredFormat = list(
            numberFormat = list(type = "NUMBER", pattern = "0")
          )
        ),
        fields = "userEnteredFormat.numberFormat"
      )
    )
  }

  for (col_idx in index_cols) {
    requests[[length(requests) + 1L]] <- list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 1L,
          endRowIndex = used_rows,
          startColumnIndex = as.integer(col_idx - 1L),
          endColumnIndex = as.integer(col_idx)
        ),
        cell = list(
          userEnteredFormat = list(
            numberFormat = list(type = "NUMBER", pattern = "0.00")
          )
        ),
        fields = "userEnteredFormat.numberFormat"
      )
    )
  }

  if (length(text_cols) > 0) {
    for (col_idx in text_cols) {
      requests[[length(requests) + 1L]] <- list(
        repeatCell = list(
          range = list(
            sheetId = sheet_id,
            startRowIndex = 1L,
            endRowIndex = used_rows,
            startColumnIndex = as.integer(col_idx - 1L),
            endColumnIndex = as.integer(col_idx)
          ),
          cell = list(
            userEnteredFormat = list(
              horizontalAlignment = "LEFT"
            )
          ),
          fields = "userEnteredFormat.horizontalAlignment"
        )
      )
    }
  }

  # Known-effects specific readability and directional coding.
  if (!is.na(team_col) && any(grepl("^Table [12]:", as.character(dat[[team_col]])))) {
    title_rows <- which(grepl("^Table [12]:", as.character(dat[[team_col]])))
    subtitle_rows <- title_rows + 1L
    title_colors <- list(
      list(red = 0.97, green = 0.92, blue = 0.86), # warm tint for Table 1
      list(red = 0.88, green = 0.93, blue = 0.98)  # cool tint for Table 2
    )
    for (k in seq_along(title_rows)) {
      tr <- as.integer(title_rows[[k]])
      clr <- title_colors[[min(k, length(title_colors))]]
      requests[[length(requests) + 1L]] <- list(
        repeatCell = list(
          range = list(
            sheetId = sheet_id,
            startRowIndex = as.integer(tr),
            endRowIndex = as.integer(tr + 1L),
            startColumnIndex = 0L,
            endColumnIndex = used_cols
          ),
          cell = list(
            userEnteredFormat = list(
              textFormat = list(bold = TRUE),
              backgroundColor = clr,
              horizontalAlignment = "LEFT"
            )
          ),
          fields = "userEnteredFormat(textFormat.bold,backgroundColor,horizontalAlignment)"
        )
      )
    }
    for (sr in subtitle_rows) {
      if (is.finite(sr) && sr <= nrow(dat)) {
        requests[[length(requests) + 1L]] <- list(
          repeatCell = list(
            range = list(
              sheetId = sheet_id,
              startRowIndex = as.integer(sr),
              endRowIndex = as.integer(sr + 1L),
              startColumnIndex = 0L,
              endColumnIndex = used_cols
            ),
            cell = list(
              userEnteredFormat = list(
                textFormat = list(italic = TRUE),
                horizontalAlignment = "LEFT"
              )
            ),
            fields = "userEnteredFormat(textFormat.italic,horizontalAlignment)"
          )
        )
      }
    }
  }

  if (!is.na(notes_col)) {
    data_start_row <- 1L
    data_end_row <- used_rows
    note_rng <- list(
      sheetId = sheet_id,
      startRowIndex = data_start_row,
      endRowIndex = data_end_row,
      startColumnIndex = as.integer(notes_col - 1L),
      endColumnIndex = as.integer(notes_col)
    )

    add_note_rule <- function(pattern, red, green, blue) {
      requests[[length(requests) + 1L]] <<- list(
        addConditionalFormatRule = list(
          index = 0L,
          rule = list(
            ranges = list(note_rng),
            booleanRule = list(
              condition = list(
                type = "CUSTOM_FORMULA",
                values = list(
                  list(userEnteredValue = sprintf("=REGEXMATCH($%s2,\"%s\")", LETTERS[notes_col], pattern))
                )
              ),
              format = list(
                backgroundColor = list(red = red, green = green, blue = blue)
              )
            )
          )
        )
      )
    }

    add_note_rule("HR-leaning", 0.98, 0.88, 0.86)
    add_note_rule("BACON-leaning", 0.89, 0.95, 0.89)
    add_note_rule("More hitter-friendly in 2H", 0.99, 0.93, 0.84)
    add_note_rule("More pitcher-friendly in 2H", 0.87, 0.92, 0.98)
  }

  if (!is.na(total_bbe_col)) {
    requests[[length(requests) + 1L]] <- list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 1L,
          endRowIndex = used_rows,
          startColumnIndex = as.integer(total_bbe_col - 1L),
          endColumnIndex = as.integer(total_bbe_col)
        ),
        cell = list(
          userEnteredFormat = list(
            numberFormat = list(type = "NUMBER", pattern = "0")
          )
        ),
        fields = "userEnteredFormat.numberFormat"
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
  final_rule_count <- get_conditional_rule_count()
  message(sprintf(
    "Conditional format rules on '%s': before=%s after=%s index_cols=%s",
    tab_name,
    existing_rule_count,
    final_rule_count,
    length(index_cols)
  ))
  invisible(TRUE)
}

export_pf_to_google_sheet <- function(dat, sheet_url, tab_name, auth_first = TRUE, freeze_header = TRUE, known_mode = FALSE) {
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required for Google Sheets export.")
  }
  if (isTRUE(auth_first)) {
    source(file.path("R", "gsheets_auth.R"))
    auth_google_sheets()
  }

  dat_out <- dat
  idx_cols <- names(dat_out)[
    grepl("_IDX_100$", names(dat_out)) |
      grepl("Park[.]?Factor$", names(dat_out))
  ]
  numeric_cols <- unique(c(
    "Rank",
    "Total BBE",
    "Total_BBE",
    "Totall BBE",
    "Total.BBE",
    idx_cols,
    "Metric A",
    "Metric B",
    "Difference",
    "Abs Difference"
  ))
  for (nm in numeric_cols) {
    if (nm %in% names(dat_out)) {
      dat_out[[nm]] <- suppressWarnings(as.numeric(dat_out[[nm]]))
    }
  }

  existing <- tryCatch(
    as.data.frame(googlesheets4::read_sheet(sheet_url, sheet = tab_name), stringsAsFactors = FALSE),
    error = function(e) data.frame(stringsAsFactors = FALSE)
  )

  if (nrow(existing) > 0 && frames_equal(existing, dat_out)) {
    message(sprintf("No changes for tab '%s'; skipping write.", tab_name))
  } else {
    googlesheets4::sheet_write(data = dat_out, ss = sheet_url, sheet = tab_name)
    message(sprintf("Updated Google Sheet tab '%s' (%s rows).", tab_name, nrow(dat_out)))
  }

  googlesheets4::sheet_resize(
    ss = sheet_url,
    sheet = tab_name,
    nrow = as.integer(nrow(dat_out) + 100L),
    ncol = as.integer(ncol(dat_out) + 100L),
    exact = FALSE
  )

  apply_pf_sheet_format(sheet_url, tab_name, dat_out, freeze_header = freeze_header, known_mode = known_mode)
  message(sprintf("Applied formatting on tab '%s' (centered + auto-width + frozen header).", tab_name))
}
