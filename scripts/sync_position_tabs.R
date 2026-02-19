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

source_sheet <- if (length(args) >= 1) args[[1]] else cfg$google_sheets$source_ranks_url
target_sheet <- if (length(args) >= 2) args[[2]] else cfg$google_sheets$workbook_url
projection_tab <- if (length(args) >= 3) args[[3]] else cfg$google_sheets$projection_tab

if (!nzchar(source_sheet)) {
  source_sheet <- target_sheet
}

if (!nzchar(source_sheet) || !nzchar(target_sheet)) {
  stop("Usage: Rscript scripts/sync_position_tabs.R <source_sheet_url> <target_sheet_url> [projection_tab] [--config path]")
}

if (!requireNamespace("googlesheets4", quietly = TRUE)) {
  stop("Package 'googlesheets4' is required.")
}

normalize_key <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  x[is.na(x)] <- ""
  x <- tolower(x)
  x <- gsub("[^a-z0-9]", "", x)
  x
}

to_position_tokens <- function(pos_text) {
  if (is.na(pos_text) || !nzchar(pos_text)) {
    return(character(0))
  }
  toks <- unlist(strsplit(as.character(pos_text), ",", fixed = TRUE))
  toks <- trimws(toks)
  toks[nzchar(toks)]
}

safe_read_sheet <- function(ss, sheet_name) {
  out <- tryCatch(
    googlesheets4::read_sheet(ss, sheet = sheet_name),
    error = function(e) NULL
  )
  if (is.null(out)) {
    return(data.frame(stringsAsFactors = FALSE))
  }
  as.data.frame(out, stringsAsFactors = FALSE)
}

build_rank_lookup <- function(existing_df) {
  if (nrow(existing_df) == 0) {
    return(data.frame(key = character(0), CK.Rank = numeric(0), DS.Rank = numeric(0), stringsAsFactors = FALSE))
  }
  required <- c("Name", "Position", "CK.Rank", "DS.Rank")
  missing <- setdiff(required, names(existing_df))
  if (length(missing) > 0) {
    return(data.frame(key = character(0), CK.Rank = numeric(0), DS.Rank = numeric(0), stringsAsFactors = FALSE))
  }

  key <- paste(
    normalize_key(existing_df$Name),
    normalize_key(existing_df$Position),
    sep = "|"
  )
  out <- data.frame(
    key = key,
    CK.Rank = suppressWarnings(as.numeric(existing_df$CK.Rank)),
    DS.Rank = suppressWarnings(as.numeric(existing_df$DS.Rank)),
    stringsAsFactors = FALSE
  )
  out <- out[!duplicated(out$key), , drop = FALSE]
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
  aa <- normalize_df_for_compare(a)
  bb <- normalize_df_for_compare(b)
  identical(aa, bb)
}

message("Authenticating Google Sheets...")
auth_google_sheets()

message("Reading projection source tab from target workbook...")
proj <- as.data.frame(
  googlesheets4::read_sheet(target_sheet, sheet = projection_tab),
  stringsAsFactors = FALSE
)

required_proj_cols <- c(
  "player_name", "position", "adp", "pa", "hr", "sb", "r", "rbi", "avg",
  "dollars_raw", "dollars_weights", "dollars_adp"
)
missing_proj <- setdiff(required_proj_cols, names(proj))
if (length(missing_proj) > 0) {
  stop(sprintf(
    "Missing required projection columns in tab '%s': %s",
    projection_tab,
    paste(missing_proj, collapse = ", ")
  ))
}

position_tabs <- c("C", "1B", "2B", "3B", "SS", "OF")

target_tab_names <- googlesheets4::sheet_names(target_sheet)
source_tab_names <- if (identical(source_sheet, target_sheet)) target_tab_names else googlesheets4::sheet_names(source_sheet)

for (pos_tab in position_tabs) {
  message(sprintf("Building tab: %s", pos_tab))

  keep <- vapply(
    proj$position,
    function(x) pos_tab %in% to_position_tokens(x),
    logical(1)
  )

  sub <- proj[keep, , drop = FALSE]
  if (nrow(sub) == 0) {
    out <- data.frame(
      Name = character(0),
      Position = character(0),
      ADP = numeric(0),
      PA = numeric(0),
      HR = numeric(0),
      SB = numeric(0),
      R = numeric(0),
      RBI = numeric(0),
      AVG = numeric(0),
      Dollars.Raw = numeric(0),
      Dollars.Weights = numeric(0),
      Dollars.ADP = numeric(0),
      Rank.Weights = numeric(0),
      Rank.ADP = numeric(0),
      CK.Rank = numeric(0),
      DS.Rank = numeric(0),
      stringsAsFactors = FALSE
    )
  } else {
    out <- data.frame(
      Name = as.character(sub$player_name),
      Position = as.character(sub$position),
      ADP = suppressWarnings(as.numeric(sub$adp)),
      PA = suppressWarnings(as.numeric(sub$pa)),
      HR = suppressWarnings(as.numeric(sub$hr)),
      SB = suppressWarnings(as.numeric(sub$sb)),
      R = suppressWarnings(as.numeric(sub$r)),
      RBI = suppressWarnings(as.numeric(sub$rbi)),
      AVG = suppressWarnings(as.numeric(sub$avg)),
      Dollars.Raw = suppressWarnings(as.numeric(sub$dollars_raw)),
      Dollars.Weights = suppressWarnings(as.numeric(sub$dollars_weights)),
      Dollars.ADP = suppressWarnings(as.numeric(sub$dollars_adp)),
      stringsAsFactors = FALSE
    )

    out$Rank.Weights <- rank(-out$Dollars.Weights, ties.method = "min", na.last = "keep")
    out$Rank.ADP <- rank(out$ADP, ties.method = "min", na.last = "keep")
    out <- out[order(out$Rank.Weights, out$Rank.ADP, na.last = TRUE), , drop = FALSE]
    rownames(out) <- NULL

    existing_target <- if (pos_tab %in% target_tab_names) safe_read_sheet(target_sheet, pos_tab) else data.frame(stringsAsFactors = FALSE)
    existing_source <- if (pos_tab %in% source_tab_names) safe_read_sheet(source_sheet, pos_tab) else data.frame(stringsAsFactors = FALSE)

    lookup_target <- build_rank_lookup(existing_target)
    lookup_source <- build_rank_lookup(existing_source)

    key_out <- paste(normalize_key(out$Name), normalize_key(out$Position), sep = "|")

    out$CK.Rank <- NA_real_
    out$DS.Rank <- NA_real_

    if (nrow(lookup_source) > 0) {
      idx_src <- match(key_out, lookup_source$key)
      has_src <- !is.na(idx_src)
      out$CK.Rank[has_src] <- lookup_source$CK.Rank[idx_src[has_src]]
      out$DS.Rank[has_src] <- lookup_source$DS.Rank[idx_src[has_src]]
    }

    if (nrow(lookup_target) > 0) {
      idx_tgt <- match(key_out, lookup_target$key)
      has_tgt <- !is.na(idx_tgt)
      out$CK.Rank[has_tgt] <- lookup_target$CK.Rank[idx_tgt[has_tgt]]
      out$DS.Rank[has_tgt] <- lookup_target$DS.Rank[idx_tgt[has_tgt]]
    }
  }

  existing_target_full <- if (pos_tab %in% target_tab_names) safe_read_sheet(target_sheet, pos_tab) else data.frame(stringsAsFactors = FALSE)
  if (nrow(existing_target_full) > 0 && frames_equal(existing_target_full, out)) {
    message(sprintf("No changes for tab '%s'; skipping write.", pos_tab))
    next
  }

  googlesheets4::sheet_write(out, ss = target_sheet, sheet = pos_tab)
}

message("Position tabs synced.")
