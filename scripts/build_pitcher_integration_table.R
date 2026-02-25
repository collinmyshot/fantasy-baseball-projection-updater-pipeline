#!/usr/bin/env Rscript

args_raw <- commandArgs(trailingOnly = TRUE)

config_path <- file.path("config", "pipeline.yml")
season_arg <- ""
projection_json_arg <- ""
adp_tsv_arg <- ""
eno_csv_arg <- ""
ck_csv_arg <- ""
sp2025_csv_arg <- ""
sp2026_csv_arg <- ""
out_csv_arg <- ""
refresh_projection_arg <- FALSE
pitcher_system_arg <- ""
sheet_url_arg <- ""
integrated_tab_arg <- ""
sp_skillz_tab_arg <- ""
no_sheet_export_arg <- FALSE

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
  if (arg == "--season" && i < length(args_raw)) {
    season_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--season=")) {
    season_arg <- sub("^--season=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--projection-json" && i < length(args_raw)) {
    projection_json_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--projection-json=")) {
    projection_json_arg <- sub("^--projection-json=", "", arg)
    i <- i + 1
    next
  }
  # Backward-compatible alias.
  if (arg == "--oopsy-json" && i < length(args_raw)) {
    projection_json_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--oopsy-json=")) {
    projection_json_arg <- sub("^--oopsy-json=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--adp-tsv" && i < length(args_raw)) {
    adp_tsv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--adp-tsv=")) {
    adp_tsv_arg <- sub("^--adp-tsv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--eno-csv" && i < length(args_raw)) {
    eno_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--eno-csv=")) {
    eno_csv_arg <- sub("^--eno-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--ck-csv" && i < length(args_raw)) {
    ck_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--ck-csv=")) {
    ck_csv_arg <- sub("^--ck-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--sp2025-csv" && i < length(args_raw)) {
    sp2025_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--sp2025-csv=")) {
    sp2025_csv_arg <- sub("^--sp2025-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--sp2026-csv" && i < length(args_raw)) {
    sp2026_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--sp2026-csv=")) {
    sp2026_csv_arg <- sub("^--sp2026-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--out-csv" && i < length(args_raw)) {
    out_csv_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--out-csv=")) {
    out_csv_arg <- sub("^--out-csv=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--refresh-projections") {
    refresh_projection_arg <- TRUE
    i <- i + 1
    next
  }
  # Backward-compatible alias.
  if (arg == "--refresh-oopsy") {
    refresh_projection_arg <- TRUE
    i <- i + 1
    next
  }
  if (arg == "--pitcher-system" && i < length(args_raw)) {
    pitcher_system_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--pitcher-system=")) {
    pitcher_system_arg <- sub("^--pitcher-system=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--sheet-url" && i < length(args_raw)) {
    sheet_url_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--sheet-url=")) {
    sheet_url_arg <- sub("^--sheet-url=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--integrated-tab" && i < length(args_raw)) {
    integrated_tab_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--integrated-tab=")) {
    integrated_tab_arg <- sub("^--integrated-tab=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--sp-skillz-tab" && i < length(args_raw)) {
    sp_skillz_tab_arg <- args_raw[[i + 1]]
    i <- i + 2
    next
  }
  if (startsWith(arg, "--sp-skillz-tab=")) {
    sp_skillz_tab_arg <- sub("^--sp-skillz-tab=", "", arg)
    i <- i + 1
    next
  }
  if (arg == "--no-sheet-export") {
    no_sheet_export_arg <- TRUE
    i <- i + 1
    next
  }
  stop(sprintf("Unknown argument: %s", arg))
}

source(file.path("R", "pipeline_config.R"))
source(file.path("R", "fangraphs_projections.R"))
source(file.path("R", "gsheets_auth.R"))

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}

PITCHER_PROJECTION_TYPES <- list(
  thebat = c("thebat"),
  steamer = c("steamer"),
  oopsy = c("oopsy"),
  atc = c("atc"),
  zips = c("zips")
)

parse_num <- function(x, fallback) {
  if (!nzchar(x)) {
    return(fallback)
  }
  out <- suppressWarnings(as.numeric(trimws(x)))
  if (length(out) != 1 || is.na(out)) {
    stop(sprintf("Invalid numeric value: %s", x))
  }
  out
}

name_key <- function(x) normalize_join_name(x)
team_key <- function(x) normalize_team_abbrev(x)

build_pitcher_projection_url <- function(season, projection_type) {
  params <- list(
    pos = "all",
    stats = "pit",
    lg = "all",
    qual = "0",
    type = projection_type,
    season = as.character(season),
    month = "0",
    season1 = as.character(season),
    ind = "0",
    team = "0",
    rost = "0",
    age = "0",
    filter = "",
    players = "0",
    z = as.character(as.integer(Sys.time())),
    sort = "0,1"
  )
  query <- paste(
    paste0(names(params), "=", vapply(params, utils::URLencode, character(1), reserved = TRUE)),
    collapse = "&"
  )
  paste0(FG_API_BASE, "?", query)
}

refresh_pitcher_projection_json <- function(json_path, season, system_name) {
  system_name <- tolower(trimws(as.character(system_name)))
  if (!system_name %in% names(PITCHER_PROJECTION_TYPES)) {
    stop(sprintf("Unsupported pitcher projection system '%s'. Allowed: %s", system_name, paste(names(PITCHER_PROJECTION_TYPES), collapse = ", ")))
  }

  projection_types <- PITCHER_PROJECTION_TYPES[[system_name]]
  last_error <- "No payload returned."
  payload <- NULL
  payload_df <- NULL

  for (projection_type in projection_types) {
    url <- build_pitcher_projection_url(season = season, projection_type = projection_type)
    fetch <- fetch_fg_json_with_fallback(
      url = url,
      simplifyVector = TRUE
    )
    payload_try <- fetch$payload
    if (!isTRUE(fetch$ok)) {
      last_error <- fetch$error
    }
    payload_df_try <- as_data_frame(payload_try)
    if (!is.null(payload_df_try) && nrow(payload_df_try) > 0) {
      payload <- payload_try
      payload_df <- payload_df_try
      break
    }
  }

  if (is.null(payload) || is.null(payload_df) || nrow(payload_df) == 0) {
    stop(sprintf("Failed to fetch pitcher projections for system '%s': %s", system_name, last_error))
  }

  dir.create(dirname(json_path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(payload, path = json_path, auto_unbox = TRUE, pretty = FALSE, na = "null")
  invisible(json_path)
}

read_pitcher_projections <- function(json_path, system_name = "oopsy") {
  if (!file.exists(json_path)) {
    stop(sprintf("Missing pitcher projections JSON for system '%s': %s", system_name, json_path))
  }
  payload <- jsonlite::fromJSON(json_path, simplifyVector = TRUE)
  raw <- NULL
  if (is.data.frame(payload)) {
    raw <- payload
  } else if (is.list(payload) && "data" %in% names(payload)) {
    raw <- as.data.frame(payload$data, stringsAsFactors = FALSE, check.names = FALSE)
  }
  if (is.null(raw)) {
    stop(sprintf("Unrecognized pitcher projection JSON shape for system '%s'.", system_name))
  }
  if (nrow(raw) == 0) {
    stop(sprintf("Pitcher projection payload has zero rows for system '%s'.", system_name))
  }

  out <- data.frame(
    player_id_fg = suppressWarnings(as.numeric(raw$playerid)),
    player_id_mlbam = suppressWarnings(as.numeric(raw$xMLBAMID)),
    player_name = trimws(as.character(raw$PlayerName)),
    team = toupper(trimws(as.character(raw$Team))),
    proj_w = suppressWarnings(as.numeric(raw$W)),
    proj_sv = suppressWarnings(as.numeric(raw$SV)),
    proj_k = suppressWarnings(as.numeric(raw$SO)),
    proj_gs = suppressWarnings(as.numeric(raw$GS)),
    proj_ip = suppressWarnings(as.numeric(raw$IP)),
    proj_era = suppressWarnings(as.numeric(raw$ERA)),
    proj_whip = suppressWarnings(as.numeric(raw$WHIP)),
    stringsAsFactors = FALSE
  )

  out$player_name[is.na(out$player_name)] <- ""
  out <- out[nzchar(out$player_name), , drop = FALSE]
  out$name_key <- name_key(out$player_name)
  out$team_key <- team_key(out$team)

  out
}

read_adp_pitchers <- function(tsv_path) {
  if (!file.exists(tsv_path)) {
    stop(sprintf("Missing ADP TSV: %s", tsv_path))
  }
  raw <- utils::read.delim(tsv_path, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE)
  if (nrow(raw) == 0) {
    return(data.frame(stringsAsFactors = FALSE))
  }

  player <- nfbc_player_name_to_first_last(raw[[3]])
  team <- trimws(as.character(raw[[4]]))
  positions <- trimws(as.character(raw[[5]]))
  adp <- suppressWarnings(as.numeric(raw[[6]]))
  adp_rank <- suppressWarnings(as.numeric(raw[[1]]))
  adp_min <- suppressWarnings(as.numeric(raw[[7]]))
  adp_max <- suppressWarnings(as.numeric(raw[[8]]))
  adp_picks <- suppressWarnings(as.numeric(raw[[10]]))

  keep <- vapply(seq_len(nrow(raw)), function(i) {
    pos <- tolower(positions[i])
    if (!nzchar(pos) || is.na(pos)) {
      return(FALSE)
    }
    tokens <- trimws(unlist(strsplit(pos, "[,/]", perl = TRUE)))
    tokens <- tokens[nzchar(tokens)]
    any(tokens == "p")
  }, logical(1))

  adp_has_sp <- vapply(seq_len(nrow(raw)), function(i) {
    pos <- tolower(positions[i])
    if (!nzchar(pos) || is.na(pos)) {
      return(FALSE)
    }
    tokens <- trimws(unlist(strsplit(pos, "[,/]", perl = TRUE)))
    tokens <- tokens[nzchar(tokens)]
    any(tokens == "sp")
  }, logical(1))

  adp_has_rp <- vapply(seq_len(nrow(raw)), function(i) {
    pos <- tolower(positions[i])
    if (!nzchar(pos) || is.na(pos)) {
      return(FALSE)
    }
    tokens <- trimws(unlist(strsplit(pos, "[,/]", perl = TRUE)))
    tokens <- tokens[nzchar(tokens)]
    any(tokens == "rp")
  }, logical(1))

  out <- data.frame(
    player_name = player[keep],
    team = team[keep],
    adp_positions = positions[keep],
    adp_has_sp = adp_has_sp[keep],
    adp_has_rp = adp_has_rp[keep],
    adp = adp[keep],
    adp_rank = adp_rank[keep],
    adp_min_pick = adp_min[keep],
    adp_max_pick = adp_max[keep],
    adp_picks = adp_picks[keep],
    stringsAsFactors = FALSE
  )
  out$name_key <- name_key(out$player_name)
  out$team_key <- team_key(out$team)
  out <- out[order(out$adp, out$adp_rank, na.last = TRUE), , drop = FALSE]
  out <- out[!duplicated(paste(out$name_key, out$team_key, sep = "|")), , drop = FALSE]
  rownames(out) <- NULL
  out
}

flag_probable_starter <- function(df) {
  adp_has_sp <- if ("adp_has_sp" %in% names(df)) as.logical(df$adp_has_sp) else rep(FALSE, nrow(df))
  adp_has_rp <- if ("adp_has_rp" %in% names(df)) as.logical(df$adp_has_rp) else rep(FALSE, nrow(df))
  has_skillz <- (!is.na(df$sp_skillz_2025_rank) | !is.na(df$sp_skillz_2026_rank))
  missing_skillz <- (is.na(df$sp_skillz_2025_rank) & is.na(df$sp_skillz_2026_rank))
  has_ranking <- (!is.na(df$eno_rank) | !is.na(df$ck_rank) | !is.na(df$adp))
  gs <- suppressWarnings(as.numeric(df$proj_gs))
  ip <- suppressWarnings(as.numeric(df$proj_ip))
  sv <- suppressWarnings(as.numeric(df$proj_sv))

  keep <- rep(FALSE, nrow(df))
  keep <- keep | adp_has_sp
  keep <- keep | has_skillz
  keep <- keep | (!is.na(gs) & gs >= 8)
  keep <- keep | (!is.na(ip) & ip >= 90 & (is.na(sv) | sv <= 8))
  keep <- keep | (has_ranking & !is.na(gs) & gs >= 3)
  keep <- keep | (!is.na(df$adp) & !is.na(ip) & ip >= 100)

  clear_rp <- (!is.na(sv) & sv >= 15 & (is.na(gs) | gs <= 3))
  clear_rp <- clear_rp | (adp_has_rp & !adp_has_sp & !has_skillz & (is.na(gs) | gs <= 5))
  clear_rp <- clear_rp | (!is.na(sv) & sv > 0 & (is.na(gs) | gs < 15))
  clear_rp <- clear_rp | (!is.na(df$adp) & is.na(df$eno_rank) & missing_skillz)
  keep <- keep & !clear_rp
  keep[is.na(keep)] <- FALSE
  keep
}

dedupe_name_conflicts <- function(df) {
  if (nrow(df) == 0) {
    return(df)
  }
  out <- df

  has_adp <- !is.na(out$adp)
  has_eno <- !is.na(out$eno_rank)
  has_ck <- !is.na(out$ck_rank)
  has_sp25 <- !is.na(out$sp_skillz_2025_rank)
  has_sp26 <- !is.na(out$sp_skillz_2026_rank)
  proj_ip <- suppressWarnings(as.numeric(out$proj_ip))
  proj_gs <- suppressWarnings(as.numeric(out$proj_gs))
  proj_sv <- suppressWarnings(as.numeric(out$proj_sv))

  out$.__dup_score <- 0
  out$.__dup_score <- out$.__dup_score + ifelse(has_adp, 1000, 0)
  out$.__dup_score <- out$.__dup_score + ifelse(has_eno, 700, 0)
  out$.__dup_score <- out$.__dup_score + ifelse(has_ck, 500, 0)
  out$.__dup_score <- out$.__dup_score + ifelse(has_sp26, 450, 0)
  out$.__dup_score <- out$.__dup_score + ifelse(has_sp25, 300, 0)
  out$.__dup_score <- out$.__dup_score + pmax(pmin(ifelse(is.na(proj_ip), 0, proj_ip), 220), 0)
  out$.__dup_score <- out$.__dup_score + 2 * pmax(pmin(ifelse(is.na(proj_gs), 0, proj_gs), 34), 0)
  out$.__dup_score <- out$.__dup_score - 2 * pmax(pmin(ifelse(is.na(proj_sv), 0, proj_sv), 40), 0)
  out$.__dup_score <- out$.__dup_score - ifelse(is.na(out$adp), 0, pmin(out$adp, 750) / 100)

  ip_sort <- ifelse(is.na(proj_ip), -Inf, proj_ip)
  ord <- order(out$name_key, -out$.__dup_score, is.na(out$adp), out$adp, -ip_sort, out$team, na.last = TRUE)
  out <- out[ord, , drop = FALSE]
  out <- out[!duplicated(out$name_key), , drop = FALSE]
  out$.__dup_score <- NULL
  rownames(out) <- NULL
  out
}

normalize_df_for_compare <- function(df) {
  out <- as.data.frame(df, stringsAsFactors = FALSE)
  for (nm in names(out)) {
    if (is.numeric(out[[nm]])) {
      out[[nm]] <- ifelse(is.na(out[[nm]]), NA, round(out[[nm]], 6))
    } else {
      out[[nm]] <- trimws(as.character(out[[nm]]))
      out[[nm]][out[[nm]] == ""] <- NA
    }
  }
  out
}

frames_equal <- function(a, b) {
  if (!identical(names(a), names(b))) return(FALSE)
  if (nrow(a) != nrow(b)) return(FALSE)
  identical(normalize_df_for_compare(a), normalize_df_for_compare(b))
}

apply_tab_display_format <- function(sheet_url, tab_name, dat, target_rows, target_cols) {
  props <- googlesheets4::sheet_properties(sheet_url)
  if (is.null(props) || nrow(props) == 0 || !tab_name %in% props$name) {
    return(invisible(FALSE))
  }
  row_idx <- match(tab_name, props$name)
  sheet_id <- as.integer(props$id[row_idx])
  ss_id <- as.character(unclass(googlesheets4::as_sheets_id(sheet_url)))

  freeze_cols <- match("adp_rank", names(dat))
  if (is.na(freeze_cols)) {
    freeze_cols <- 1L
  }
  freeze_cols <- as.integer(max(1L, min(as.integer(target_cols), as.integer(freeze_cols))))

  filter_rows <- as.integer(max(1L, nrow(dat) + 1L))
  filter_cols <- as.integer(max(1L, ncol(dat)))
  center_rows <- as.integer(max(1L, target_rows))
  center_cols <- as.integer(max(1L, target_cols))

  requests <- list(
    list(
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
    ),
    list(
      setBasicFilter = list(
        filter = list(
          range = list(
            sheetId = sheet_id,
            startRowIndex = 0L,
            endRowIndex = filter_rows,
            startColumnIndex = 0L,
            endColumnIndex = filter_cols
          )
        )
      )
    ),
    list(
      repeatCell = list(
        range = list(
          sheetId = sheet_id,
          startRowIndex = 0L,
          endRowIndex = center_rows,
          startColumnIndex = 0L,
          endColumnIndex = center_cols
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
  )

  req <- googlesheets4::request_generate(
    "sheets.spreadsheets.batchUpdate",
    params = list(
      spreadsheetId = ss_id,
      requests = requests
    )
  )
  googlesheets4::request_make(req)
  message(sprintf(
    "Applied filter/center/freeze formatting on tab '%s' (freeze cols through adp_rank=%s).",
    tab_name, freeze_cols
  ))
  invisible(TRUE)
}

write_tab_if_changed <- function(sheet_url, tab_name, dat) {
  existing <- tryCatch(
    as.data.frame(googlesheets4::read_sheet(sheet_url, sheet = tab_name), stringsAsFactors = FALSE),
    error = function(e) data.frame(stringsAsFactors = FALSE)
  )
  if (nrow(existing) > 0 && frames_equal(existing, dat)) {
    message(sprintf("No changes for tab '%s'; skipping write.", tab_name))
  } else {
    googlesheets4::sheet_write(data = dat, ss = sheet_url, sheet = tab_name)
    message(sprintf("Updated tab '%s' (%s rows).", tab_name, nrow(dat)))
  }

  # Always keep expansion buffer so future writes/formulas have room.
  target_rows <- as.integer(nrow(dat) + 100L)
  target_cols <- as.integer(ncol(dat) + 100L)
  googlesheets4::sheet_resize(
    ss = sheet_url,
    sheet = tab_name,
    nrow = target_rows,
    ncol = target_cols,
    exact = FALSE
  )
  message(sprintf(
    "Resized tab '%s' to at least %s rows x %s cols.",
    tab_name, target_rows, target_cols
  ))
  apply_tab_display_format(
    sheet_url = sheet_url,
    tab_name = tab_name,
    dat = dat,
    target_rows = target_rows,
    target_cols = target_cols
  )

  invisible(TRUE)
}

export_pitcher_tables_to_gsheets <- function(sheet_url, integrated_tab, sp_skillz_tab, integrated_dat, sp_skillz_dat) {
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required for Google Sheets export.")
  }
  auth_google_sheets()
  write_tab_if_changed(sheet_url, integrated_tab, integrated_dat)
  write_tab_if_changed(sheet_url, sp_skillz_tab, sp_skillz_dat)

  # Keep SP Skillz as leftmost, with Integrated immediately to its right.
  tabs <- googlesheets4::sheet_names(sheet_url)
  if (length(tabs) > 0 && sp_skillz_tab %in% tabs) {
    leftmost <- tabs[[1]]
    if (!identical(leftmost, sp_skillz_tab)) {
      googlesheets4::sheet_relocate(sheet_url, sheet = sp_skillz_tab, .before = leftmost)
      message(sprintf("Moved tab '%s' to leftmost position.", sp_skillz_tab))
    }
  }
  tabs <- googlesheets4::sheet_names(sheet_url)
  if (sp_skillz_tab %in% tabs && integrated_tab %in% tabs) {
    googlesheets4::sheet_relocate(sheet_url, sheet = integrated_tab, .after = sp_skillz_tab)
    message(sprintf("Moved tab '%s' to the right of '%s'.", integrated_tab, sp_skillz_tab))
  }
}

read_eno_rankings <- function(csv_path) {
  if (!file.exists(csv_path)) {
    stop(sprintf("Missing Eno rankings CSV: %s", csv_path))
  }
  d <- utils::read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)
  required <- c("player_name", "eno_rank")
  missing <- setdiff(required, names(d))
  if (length(missing) > 0) {
    stop(sprintf("Eno CSV missing required columns: %s", paste(missing, collapse = ", ")))
  }
  out <- data.frame(
    player_id = suppressWarnings(as.numeric(d$player_id)),
    player_name = as.character(d$player_name),
    eno_rank = suppressWarnings(as.numeric(d$eno_rank)),
    eno_2026_stuff_plus = suppressWarnings(as.numeric(d$eno_2026_stuff_plus)),
    eno_2026_pitching_plus = suppressWarnings(as.numeric(d$eno_2026_pitching_plus)),
    stringsAsFactors = FALSE
  )
  out$name_key <- name_key(out$player_name)
  out <- out[order(out$eno_rank, out$player_name, na.last = TRUE), , drop = FALSE]
  out <- out[!duplicated(ifelse(!is.na(out$player_id), paste0("id:", out$player_id), paste0("name:", out$name_key))), , drop = FALSE]
  rownames(out) <- NULL
  out
}

read_ck_ranks <- function(csv_path) {
  if (!file.exists(csv_path)) {
    stop(sprintf("Missing CK SP ranks CSV: %s", csv_path))
  }
  d <- utils::read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)
  if (!all(c("Player", "Position(s)", "CK Rank") %in% names(d))) {
    stop("CK CSV missing required columns (Player, Position(s), CK Rank).")
  }

  player <- nfbc_player_name_to_first_last(d$Player)
  position <- tolower(trimws(as.character(d[["Position(s)"]])))
  keep <- vapply(seq_along(position), function(i) {
    tokens <- trimws(unlist(strsplit(position[i], "[,/]", perl = TRUE)))
    tokens <- tokens[nzchar(tokens)]
    any(tokens == "p")
  }, logical(1))

  out <- data.frame(
    player_name = player[keep],
    ck_rank = suppressWarnings(as.numeric(d[["CK Rank"]][keep])),
    ds_rank = suppressWarnings(as.numeric(d[["DS Rank"]][keep])),
    stringsAsFactors = FALSE
  )
  out$name_key <- name_key(out$player_name)
  out <- out[order(out$ck_rank, out$player_name, na.last = TRUE), , drop = FALSE]
  out <- out[!duplicated(out$name_key), , drop = FALSE]
  rownames(out) <- NULL
  out
}

read_sp_model <- function(csv_path, prefix) {
  if (!file.exists(csv_path)) {
    stop(sprintf("Missing SP Skillz CSV: %s", csv_path))
  }
  d <- utils::read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE)
  needed <- c("player_name", "sp_skillz_score", "sp_skillz_rank", "sp_skillz_score_stabilized", "sp_skillz_rank_stabilized", "sp_skillz_reliability")
  missing <- setdiff(needed, names(d))
  if (length(missing) > 0) {
    stop(sprintf("SP Skillz CSV missing required columns: %s", paste(missing, collapse = ", ")))
  }

  out <- data.frame(
    player_id = suppressWarnings(as.numeric(d$player_id)),
    player_name = as.character(d$player_name),
    team = as.character(d$team),
    stringsAsFactors = FALSE
  )
  out$name_key <- name_key(out$player_name)
  out$team_key <- team_key(out$team)
  out[[paste0(prefix, "_score")]] <- suppressWarnings(as.numeric(d$sp_skillz_score))
  out[[paste0(prefix, "_rank")]] <- suppressWarnings(as.numeric(d$sp_skillz_rank))
  out[[paste0(prefix, "_score_stabilized")]] <- suppressWarnings(as.numeric(d$sp_skillz_score_stabilized))
  out[[paste0(prefix, "_rank_stabilized")]] <- suppressWarnings(as.numeric(d$sp_skillz_rank_stabilized))
  out[[paste0(prefix, "_reliability")]] <- suppressWarnings(as.numeric(d$sp_skillz_reliability))

  out <- out[order(out[[paste0(prefix, "_rank_stabilized")]], out[[paste0(prefix, "_rank")]], out$player_name, na.last = TRUE), , drop = FALSE]
  out <- out[!duplicated(ifelse(!is.na(out$player_id), paste0("id:", out$player_id), paste0("name_team:", out$name_key, "|", out$team_key))), , drop = FALSE]
  rownames(out) <- NULL
  out
}

apply_source_merge <- function(base, src, cols, source_id_col = NULL, base_id_col = "player_id_mlbam") {
  out <- base
  for (col in cols) {
    if (!col %in% names(out)) {
      out[[col]] <- NA
    }
  }
  if (is.null(src) || nrow(src) == 0) {
    return(out)
  }

  unresolved <- rep(TRUE, nrow(out))

  if (!is.null(source_id_col) && source_id_col %in% names(src) && base_id_col %in% names(out)) {
    idx <- match(out[[base_id_col]], src[[source_id_col]])
    matched <- !is.na(out[[base_id_col]]) & !is.na(idx)
    if (any(matched)) {
      for (col in cols) {
        out[[col]][matched] <- src[[col]][idx[matched]]
      }
      unresolved[matched] <- FALSE
    }
  }

  if ("name_key" %in% names(src) && "team_key" %in% names(src)) {
    base_key <- paste(out$name_key, out$team_key, sep = "|")
    src_key <- paste(src$name_key, src$team_key, sep = "|")
    idx2 <- match(base_key[unresolved], src_key)
    matched2 <- !is.na(idx2)
    if (any(matched2)) {
      rows <- which(unresolved)[matched2]
      ref <- idx2[matched2]
      for (col in cols) {
        out[[col]][rows] <- src[[col]][ref]
      }
      unresolved[rows] <- FALSE
    }
  }

  if ("name_key" %in% names(src)) {
    name_tab <- table(src$name_key)
    unique_names <- names(name_tab[name_tab == 1])
    can_try <- unresolved & out$name_key %in% unique_names
    if (any(can_try)) {
      idx3 <- match(out$name_key[can_try], src$name_key)
      matched3 <- !is.na(idx3)
      if (any(matched3)) {
        rows <- which(can_try)[matched3]
        ref <- idx3[matched3]
        for (col in cols) {
          out[[col]][rows] <- src[[col]][ref]
        }
      }
    }
  }

  out
}

cfg <- load_pipeline_config(config_path)
season <- as.integer(round(parse_num(season_arg, cfg$season)))

pitcher_projection_system <- tolower(trimws(if (nzchar(pitcher_system_arg)) pitcher_system_arg else (cfg$pitcher$projection$system %||% "oopsy")))
if (!pitcher_projection_system %in% names(PITCHER_PROJECTION_TYPES)) {
  stop(sprintf("Unsupported pitcher projection system '%s'. Allowed: %s", pitcher_projection_system, paste(names(PITCHER_PROJECTION_TYPES), collapse = ", ")))
}

projection_json_path <- if (nzchar(projection_json_arg)) {
  projection_json_arg
} else {
  file.path("data", "raw", sprintf("%s_%s_pitchers_raw.json", season, pitcher_projection_system))
}
adp_tsv_path <- if (nzchar(adp_tsv_arg)) adp_tsv_arg else (cfg$adp$local_tsv %||% file.path("data", "raw", "ADP.tsv"))
eno_csv_path <- if (nzchar(eno_csv_arg)) eno_csv_arg else file.path("data", "processed", sprintf("%s_sp_skillz_eno_rankings.csv", cfg$pitcher$sp_skillz$lookback_season))
ck_csv_path <- if (nzchar(ck_csv_arg)) ck_csv_arg else file.path("data", "raw", sprintf("%s_ck_sp_ranks.csv", season))
sp2025_csv_path <- if (nzchar(sp2025_csv_arg)) sp2025_csv_arg else file.path("data", "processed", sprintf("%s_sp_skillz_scores.csv", cfg$pitcher$sp_skillz$lookback_season))
sp2026_csv_path <- if (nzchar(sp2026_csv_arg)) sp2026_csv_arg else file.path("data", "processed", sprintf("%s_sp_skillz_scores_2026_plus_model.csv", cfg$pitcher$sp_skillz$lookback_season))
out_csv_path <- if (nzchar(out_csv_arg)) out_csv_arg else file.path("data", "processed", sprintf("%s_pitchers_integrated_table.csv", season))

if (refresh_projection_arg || !file.exists(projection_json_path)) {
  refresh_pitcher_projection_json(
    json_path = projection_json_path,
    season = season,
    system_name = pitcher_projection_system
  )
}

pitcher_proj <- read_pitcher_projections(
  json_path = projection_json_path,
  system_name = pitcher_projection_system
)
pitcher_proj$projection_system <- pitcher_projection_system
adp <- read_adp_pitchers(adp_tsv_path)
eno <- read_eno_rankings(eno_csv_path)
ck <- read_ck_ranks(ck_csv_path)
sp2025 <- read_sp_model(sp2025_csv_path, prefix = "sp_skillz_2025")
sp2026 <- read_sp_model(sp2026_csv_path, prefix = "sp_skillz_2026")

base <- pitcher_proj

# Ohtani Rule: include him if any source has him, even if missing from projections.
ohtani_key <- name_key("Shohei Ohtani")
if (!any(base$name_key == ohtani_key)) {
  has_any <- any(adp$name_key == ohtani_key) || any(eno$name_key == ohtani_key) || any(ck$name_key == ohtani_key) ||
    any(sp2025$name_key == ohtani_key) || any(sp2026$name_key == ohtani_key)
  if (has_any) {
    base <- rbind(base, data.frame(
      player_id_fg = NA_real_,
      player_id_mlbam = NA_real_,
      player_name = "Shohei Ohtani",
      team = "",
      projection_system = pitcher_projection_system,
      proj_w = NA_real_,
      proj_sv = NA_real_,
      proj_k = NA_real_,
      proj_ip = NA_real_,
      proj_era = NA_real_,
      proj_whip = NA_real_,
      name_key = ohtani_key,
      team_key = "",
      stringsAsFactors = FALSE
    ))
  }
}

base <- apply_source_merge(
  base,
  src = adp,
  cols = c("adp_positions", "adp_has_sp", "adp_has_rp", "adp", "adp_rank", "adp_min_pick", "adp_max_pick", "adp_picks"),
  source_id_col = NULL
)

base <- apply_source_merge(
  base,
  src = eno,
  cols = c("eno_rank", "eno_2026_stuff_plus", "eno_2026_pitching_plus"),
  source_id_col = "player_id"
)

base <- apply_source_merge(
  base,
  src = ck,
  cols = c("ck_rank", "ds_rank"),
  source_id_col = NULL
)

base <- apply_source_merge(
  base,
  src = sp2025,
  cols = c(
    "sp_skillz_2025_score",
    "sp_skillz_2025_rank",
    "sp_skillz_2025_score_stabilized",
    "sp_skillz_2025_rank_stabilized",
    "sp_skillz_2025_reliability"
  ),
  source_id_col = "player_id"
)

base <- apply_source_merge(
  base,
  src = sp2026,
  cols = c(
    "sp_skillz_2026_score",
    "sp_skillz_2026_rank",
    "sp_skillz_2026_score_stabilized",
    "sp_skillz_2026_rank_stabilized",
    "sp_skillz_2026_reliability"
  ),
  source_id_col = "player_id"
)

base <- base[flag_probable_starter(base), , drop = FALSE]
base <- dedupe_name_conflicts(base)

# Re-rank ADP after starter filtering and dedupe, so ADP rank aligns with ordered ADP.
base$adp_rank <- rank(base$adp, ties.method = "min", na.last = "keep")

out <- base[, c(
  "player_name",
  "team",
  "adp",
  "adp_rank",
  "proj_ip",
  "proj_w",
  "proj_sv",
  "proj_k",
  "proj_era",
  "proj_whip",
  "sp_skillz_2025_score_stabilized",
  "sp_skillz_2025_rank_stabilized",
  "sp_skillz_2026_score_stabilized",
  "sp_skillz_2026_rank_stabilized",
  "eno_rank",
  "ck_rank",
  "ds_rank"
), drop = FALSE]

out$proj_era <- round(suppressWarnings(as.numeric(out$proj_era)), 2)
out$proj_whip <- round(suppressWarnings(as.numeric(out$proj_whip)), 2)
out$sp_skillz_2025_score_stabilized <- round(suppressWarnings(as.numeric(out$sp_skillz_2025_score_stabilized)), 3)
out$sp_skillz_2026_score_stabilized <- round(suppressWarnings(as.numeric(out$sp_skillz_2026_score_stabilized)), 3)

out <- out[order(is.na(out$adp), out$adp, out$sp_skillz_2026_rank_stabilized, out$player_name), , drop = FALSE]
rownames(out) <- NULL

dir.create(dirname(out_csv_path), recursive = TRUE, showWarnings = FALSE)
utils::write.csv(out, out_csv_path, row.names = FALSE, na = "")

sp_skillz_out <- out[, c(
  "player_name",
  "team",
  "adp",
  "adp_rank",
  "sp_skillz_2025_score_stabilized",
  "sp_skillz_2025_rank_stabilized",
  "sp_skillz_2026_score_stabilized",
  "sp_skillz_2026_rank_stabilized"
), drop = FALSE]
sp_skillz_out <- sp_skillz_out[order(
  is.na(sp_skillz_out$sp_skillz_2026_rank_stabilized),
  sp_skillz_out$sp_skillz_2026_rank_stabilized,
  sp_skillz_out$player_name
), , drop = FALSE]
rownames(sp_skillz_out) <- NULL

sheet_url <- if (nzchar(sheet_url_arg)) sheet_url_arg else as.character(cfg$pitcher$google_sheets$workbook_url %||% "")
integrated_tab <- if (nzchar(integrated_tab_arg)) integrated_tab_arg else as.character(cfg$pitcher$google_sheets$integrated_tab %||% "Pitcher Integrated")
sp_skillz_tab <- if (nzchar(sp_skillz_tab_arg)) sp_skillz_tab_arg else as.character(cfg$pitcher$google_sheets$sp_skillz_tab %||% "Pitcher SP Skillz")
auto_export <- isTRUE(cfg$pitcher$google_sheets$auto_export) && !isTRUE(no_sheet_export_arg)

if (auto_export && nzchar(sheet_url)) {
  export_pitcher_tables_to_gsheets(
    sheet_url = sheet_url,
    integrated_tab = integrated_tab,
    sp_skillz_tab = sp_skillz_tab,
    integrated_dat = out,
    sp_skillz_dat = sp_skillz_out
  )
} else {
  message("Google Sheets export skipped (auto_export disabled or workbook URL missing).")
}

message(sprintf("Saved integrated pitcher table (%s): %s", pitcher_projection_system, out_csv_path))
message(sprintf("Rows: %s", nrow(out)))
