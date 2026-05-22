#!/usr/bin/env Rscript
# export_abrl_sheets.R
# Export adjusted barrel (aBrl) leaderboards to Google Sheets, one tab per season.
# Pulls PA from FanGraphs API, merges with BBE-level barrel data.
# Applies Savant-style conditional formatting (blue=cold, white=avg, red=hot).

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(googlesheets4)
})

project_root <- normalizePath(file.path(dirname(
  if (interactive()) rstudioapi::getSourceEditorContext()$path
  else commandArgs(trailingOnly = FALSE) |>
    grep("--file=", x = _, value = TRUE) |>
    sub("--file=", "", x = _) |>
    normalizePath()
), ".."), mustWork = TRUE)

source(file.path(project_root, "R", "gsheets_auth.R"))

SHEET_URL <- "https://docs.google.com/spreadsheets/d/16WxkFwZYKcurMM0CJJr4l5mo1S6rs-yKnnr1ZfmoGjU/edit"
SHEET_ID <- "16WxkFwZYKcurMM0CJJr4l5mo1S6rs-yKnnr1ZfmoGjU"

# All seasons including current partial season
all_seasons <- setdiff(2015:as.integer(format(Sys.Date(), "%Y")), 2020)

# ── 1. Load barrel data ───────────────────────────────────────────────────

cat("Loading barrel hitter data...\n")
hitters <- read_csv(file.path(project_root, "data", "processed", "adjusted_barrel_hitters.csv"),
                    show_col_types = FALSE)

# Filter to seasons that actually exist in the data
seasons <- intersect(all_seasons, unique(hitters$season))
cat(sprintf("  Seasons in data: %s\n", paste(seasons, collapse = ", ")))

# ── 2. Filter and compute rates ───────────────────────────────────────────
# PA is already in the hitters CSV (fetched from FanGraphs during build)

merged <- hitters |>
  filter(!is.na(pa), pa >= 50)

cat(sprintf("  %d hitter-seasons with PA >= 50\n", nrow(merged)))

# Compute all rate columns
merged <- merged |>
  mutate(
    `Brl/BBE%` = round(tango_barrels / total_bbe * 100, 1),
    `aBrl/BBE%` = round(adj_barrels / total_bbe * 100, 1),
    `Brl/PA%` = round(tango_barrels / pa * 100, 1),
    `aBrl/PA%` = round(adj_barrels / pa * 100, 1),
    `Diff (Brl/BBE)` = round(`aBrl/BBE%` - `Brl/BBE%`, 1),
    `Diff (Brl/PA)` = round(`aBrl/PA%` - `Brl/PA%`, 1),
    `% Lost` = ifelse(tango_barrels > 0,
                      round((tango_barrels - adj_barrels) / tango_barrels * 100, 0),
                      0)
  )

# ── 4. Determine 2026 stability label ────────────────────────────────────

current_season <- max(seasons)
convergence_path <- file.path(project_root, "data", "processed", "adjusted_barrel_convergence.csv")

get_stability_label <- function(season, conv_path) {
  if (season < 2026) return(as.character(season))

  # Count unique game dates in the BBE store for this season
  bbe_store_path <- file.path(project_root, "data", "raw", "statcast_bbe_store.csv")
  games_played <- NA
  if (file.exists(bbe_store_path)) {
    # Extract unique game dates for this season
    dates <- system(sprintf("grep ',%d$' '%s' | cut -d',' -f2 | sort -u", season, bbe_store_path), intern = TRUE)
    dates <- dates[nchar(dates) > 0 & dates != "\"game_date\""]
    games_played <- length(dates)
  }

  # Stabilization point: ~115 games (convergence analysis shows <0.2 mph error by ~70% of season)
  stability_games <- 115

  if (!is.na(games_played) && games_played > 0) {
    pct_to_stability <- min(round(games_played / stability_games * 100, 0), 100)
    sprintf("%d (%d%% to stability)", season, pct_to_stability)
  } else {
    sprintf("%d (partial)", season)
  }
}

# ── 5. Authenticate and write to Google Sheets ───────────────────────────

cat("Authenticating with Google Sheets...\n")
auth_google_sheets()

ss <- gs4_get(SHEET_ID)
cat(sprintf("Writing to: %s\n", ss$name))

# Get existing sheet names — delete all existing to rewrite in correct order
existing_sheets <- sheet_names(SHEET_ID)

# Write tabs in reverse chronological order (most recent first)
seasons_ordered <- sort(seasons, decreasing = TRUE)

# Track sheet IDs for conditional formatting
sheet_ids <- list()

for (s in seasons_ordered) {
  tab_name <- get_stability_label(s)
  tab_data <- merged |>
    filter(season == s) |>
    arrange(desc(`Brl/BBE%`)) |>
    select(
      Player = player_name,
      PA = pa,
      BBE = total_bbe,
      Barrels = tango_barrels,
      aBarrels = adj_barrels,
      `Brl/BBE%`,
      `aBrl/BBE%`,
      `Diff (Brl/BBE)`,
      `Brl/PA%`,
      `aBrl/PA%`,
      `Diff (Brl/PA)`,
      `% Lost`
    )

  cat(sprintf("  %s: %d players...\n", tab_name, nrow(tab_data)))

  if (tab_name %in% existing_sheets) {
    sheet_write(tab_data, ss = SHEET_ID, sheet = tab_name)
  } else {
    sheet_add(SHEET_ID, sheet = tab_name)
    sheet_write(tab_data, ss = SHEET_ID, sheet = tab_name)
  }
  Sys.sleep(1)
}

# Remove old tabs that shouldn't exist
current_tabs <- sheet_names(SHEET_ID)
expected_tabs <- sapply(seasons_ordered, get_stability_label)
for (old_tab in setdiff(current_tabs, expected_tabs)) {
  cat(sprintf("  Removing old tab: %s\n", old_tab))
  tryCatch(sheet_delete(SHEET_ID, sheet = old_tab), error = function(e) NULL)
}

# Reorder tabs: most recent first
cat("Reordering tabs...\n")
current_tabs <- sheet_names(SHEET_ID)
desired_order <- expected_tabs[expected_tabs %in% current_tabs]

# Get sheet metadata for reordering
ss_meta <- gs4_get(SHEET_ID)
sheet_props <- ss_meta$sheets

# Build batch update requests to reorder
reorder_requests <- lapply(seq_along(desired_order), function(i) {
  tab <- desired_order[i]
  sid <- sheet_props$id[sheet_props$name == tab]
  if (length(sid) == 0) return(NULL)
  list(updateSheetProperties = list(
    properties = list(sheetId = sid, index = i - 1L),
    fields = "index"
  ))
})
reorder_requests <- Filter(Negate(is.null), reorder_requests)

if (length(reorder_requests) > 0) {
  req <- request_generate(
    endpoint = "sheets.spreadsheets.batchUpdate",
    params = list(
      spreadsheetId = SHEET_ID,
      requests = reorder_requests
    )
  )
  request_make(req)
}

# ── 6. Apply Savant-style conditional formatting ─────────────────────────

cat("Applying conditional formatting...\n")

# Savant colors: blue (cold) -> white -> red (hot)
savant_blue <- list(red = 33/255, green = 102/255, blue = 172/255)
savant_white <- list(red = 1, green = 1, blue = 1)
savant_red  <- list(red = 1, green = 59/255, blue = 48/255)

# Refresh metadata after writes
ss_meta <- gs4_get(SHEET_ID)
sheet_props <- ss_meta$sheets

# Column indices (0-based): Player=0, PA=1, BBE=2, Barrels=3, aBarrels=4,
# Brl/BBE%=5, aBrl/BBE%=6, Diff(Brl/BBE)=7, Brl/PA%=8, aBrl/PA%=9,
# Diff(Brl/PA)=10, %Lost=11

format_requests <- list()

for (tab_name in desired_order) {
  sid <- sheet_props$id[sheet_props$name == tab_name]
  if (length(sid) == 0) next

  n_rows <- nrow(merged |> filter(season == as.integer(sub(" .*", "", tab_name))))

  # Rate columns: Brl/BBE% (5), aBrl/BBE% (6), Brl/PA% (8), aBrl/PA% (9) — red=hot=good
  for (col_idx in c(5, 6, 8, 9)) {
    format_requests <- c(format_requests, list(list(
      addConditionalFormatRule = list(
        rule = list(
          ranges = list(list(
            sheetId = sid,
            startRowIndex = 1, endRowIndex = n_rows + 1,
            startColumnIndex = col_idx, endColumnIndex = col_idx + 1
          )),
          gradientRule = list(
            minpoint = list(type = "MIN", color = savant_blue),
            midpoint = list(type = "PERCENTILE", value = "50", color = savant_white),
            maxpoint = list(type = "MAX", color = savant_red)
          )
        ),
        index = 0
      )
    )))
  }

  # Diff columns: Diff(Brl/BBE) (7), Diff(Brl/PA) (10) — these are negative, more negative = bigger change
  # Red=hot=good (less negative = closer to 0 = less impact)
  for (col_idx in c(7, 10)) {
    format_requests <- c(format_requests, list(list(
      addConditionalFormatRule = list(
        rule = list(
          ranges = list(list(
            sheetId = sid,
            startRowIndex = 1, endRowIndex = n_rows + 1,
            startColumnIndex = col_idx, endColumnIndex = col_idx + 1
          )),
          gradientRule = list(
            minpoint = list(type = "MIN", color = savant_blue),
            midpoint = list(type = "PERCENTILE", value = "50", color = savant_white),
            maxpoint = list(type = "MAX", color = savant_red)
          )
        ),
        index = 0
      )
    )))
  }

  # % Lost (11): high = blue (bad), low = white
  format_requests <- c(format_requests, list(list(
    addConditionalFormatRule = list(
      rule = list(
        ranges = list(list(
          sheetId = sid,
          startRowIndex = 1, endRowIndex = n_rows + 1,
          startColumnIndex = 11, endColumnIndex = 12
        )),
        gradientRule = list(
          minpoint = list(type = "MIN", color = savant_white),
          midpoint = list(type = "PERCENTILE", value = "50", color = savant_white),
          maxpoint = list(type = "MAX", color = savant_blue)
        )
      ),
      index = 0
    )
  )))
}

# Send all formatting in one batch
if (length(format_requests) > 0) {
  # Process in chunks to avoid API limits
  chunk_size <- 50
  chunks <- split(format_requests, ceiling(seq_along(format_requests) / chunk_size))
  for (i in seq_along(chunks)) {
    cat(sprintf("  Formatting batch %d/%d...\n", i, length(chunks)))
    req <- request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params = list(
        spreadsheetId = SHEET_ID,
        requests = chunks[[i]]
      )
    )
    request_make(req)
    Sys.sleep(0.5)
  }
}

# ── 7. Apply cell formatting: center align + number format ────────────────

cat("Applying cell formatting (alignment + number format)...\n")

cell_format_requests <- list()

for (tab_name in desired_order) {
  sid <- sheet_props$id[sheet_props$name == tab_name]
  if (length(sid) == 0) next

  n_rows <- nrow(merged |> filter(season == as.integer(sub(" .*", "", tab_name))))

  # Center align ALL columns (including header row)
  cell_format_requests <- c(cell_format_requests, list(list(
    repeatCell = list(
      range = list(
        sheetId = sid,
        startRowIndex = 0, endRowIndex = n_rows + 1,
        startColumnIndex = 0, endColumnIndex = 12
      ),
      cell = list(
        userEnteredFormat = list(
          horizontalAlignment = "CENTER"
        )
      ),
      fields = "userEnteredFormat.horizontalAlignment"
    )
  )))

  # Integer format for PA(1), BBE(2), Barrels(3), aBarrels(4), %Lost(11)
  for (col_idx in c(1, 2, 3, 4, 11)) {
    cell_format_requests <- c(cell_format_requests, list(list(
      repeatCell = list(
        range = list(
          sheetId = sid,
          startRowIndex = 1, endRowIndex = n_rows + 1,
          startColumnIndex = col_idx, endColumnIndex = col_idx + 1
        ),
        cell = list(
          userEnteredFormat = list(
            numberFormat = list(type = "NUMBER", pattern = "0")
          )
        ),
        fields = "userEnteredFormat.numberFormat"
      )
    )))
  }

  # 1-decimal format for rate columns: Brl/BBE%(5), aBrl/BBE%(6), Diff(7), Brl/PA%(8), aBrl/PA%(9), Diff(10)
  for (col_idx in c(5, 6, 7, 8, 9, 10)) {
    cell_format_requests <- c(cell_format_requests, list(list(
      repeatCell = list(
        range = list(
          sheetId = sid,
          startRowIndex = 1, endRowIndex = n_rows + 1,
          startColumnIndex = col_idx, endColumnIndex = col_idx + 1
        ),
        cell = list(
          userEnteredFormat = list(
            numberFormat = list(type = "NUMBER", pattern = "0.0")
          )
        ),
        fields = "userEnteredFormat.numberFormat"
      )
    )))
  }
}

# Send cell formatting in chunks
if (length(cell_format_requests) > 0) {
  chunk_size <- 50
  chunks <- split(cell_format_requests, ceiling(seq_along(cell_format_requests) / chunk_size))
  for (i in seq_along(chunks)) {
    cat(sprintf("  Cell format batch %d/%d...\n", i, length(chunks)))
    req <- request_generate(
      endpoint = "sheets.spreadsheets.batchUpdate",
      params = list(
        spreadsheetId = SHEET_ID,
        requests = chunks[[i]]
      )
    )
    request_make(req)
    Sys.sleep(0.5)
  }
}

cat(sprintf("\nDone! Sheet: %s\n", SHEET_URL))
