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
tab_name <- if (length(args) >= 2) args[[2]] else cfg$google_sheets$rbll_tab
projection_tab <- if (length(args) >= 3) args[[3]] else cfg$google_sheets$projection_tab

if (!nzchar(target_sheet)) {
  stop("Usage: Rscript scripts/setup_rbell_team_targets.R <target_sheet_url> [tab_name] [projection_tab] [--config path]")
}

if (!requireNamespace("googlesheets4", quietly = TRUE)) {
  stop("Package 'googlesheets4' is required.")
}

gs4f <- googlesheets4::gs4_formula

safe_read <- function(ss, sheet, range) {
  out <- tryCatch(
    googlesheets4::read_sheet(ss, sheet = sheet, range = range, col_names = FALSE),
    error = function(e) NULL
  )
  if (is.null(out)) {
    return(NULL)
  }
  as.data.frame(out, stringsAsFactors = FALSE)
}

has_any_value <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(FALSE)
  }
  vals <- as.character(unlist(df, use.names = FALSE))
  vals[is.na(vals)] <- ""
  any(nzchar(vals))
}

row_values <- function(df, expected_len) {
  if (is.null(df) || nrow(df) == 0) {
    return(rep("", expected_len))
  }
  vals <- as.character(unlist(df[1, ], use.names = FALSE))
  vals[is.na(vals)] <- ""
  if (length(vals) < expected_len) {
    vals <- c(vals, rep("", expected_len - length(vals)))
  } else if (length(vals) > expected_len) {
    vals <- vals[seq_len(expected_len)]
  }
  vals
}

non_comment_nonempty_count <- function(vals) {
  vals <- trimws(vals)
  sum(nzchar(vals) & !grepl("^#", vals))
}

choose_target_row <- function(primary_df, secondary_df, defaults) {
  expected_len <- length(defaults)
  primary_vals <- row_values(primary_df, expected_len)
  secondary_vals <- row_values(secondary_df, expected_len)

  primary_first <- trimws(primary_vals[1])
  secondary_first <- trimws(secondary_vals[1])
  primary_has_first <- nzchar(primary_first) && !grepl("^#", primary_first)
  secondary_has_first <- nzchar(secondary_first) && !grepl("^#", secondary_first)

  # Legacy sheets sometimes have targets shifted one column right.
  if (!primary_has_first && secondary_has_first) {
    return(secondary_df)
  }

  if (non_comment_nonempty_count(secondary_vals) > non_comment_nonempty_count(primary_vals)) {
    return(secondary_df)
  }

  primary_df
}

preserve_targets <- function(df, defaults) {
  if (is.null(df) || nrow(df) == 0) {
    return(defaults)
  }
  vals <- row_values(df, length(defaults))
  out <- defaults
  for (i in seq_along(vals)) {
    val <- trimws(vals[i])
    if (nzchar(val) && !grepl("^#", val)) {
      out[i] <- val
    }
  }
  out
}

flag_implausible_targets <- function(hit_targets, pit_targets) {
  issues <- character(0)

  check_numeric <- function(values, idx, label, min_val, max_val) {
    raw <- trimws(values[idx])
    if (!nzchar(raw) || grepl("^#", raw)) {
      return(NULL)
    }
    val <- suppressWarnings(as.numeric(raw))
    if (is.na(val)) {
      return(sprintf("%s is non-numeric ('%s')", label, raw))
    }
    if (val < min_val || val > max_val) {
      return(sprintf("%s = %s is outside expected range [%s, %s]", label, raw, min_val, max_val))
    }
    NULL
  }

  checks <- list(
    list(vec = hit_targets, idx = 1, label = "Hitter HR target", min = 0, max = 800),
    list(vec = hit_targets, idx = 2, label = "Hitter SB target", min = 0, max = 800),
    list(vec = hit_targets, idx = 3, label = "Hitter R target", min = 0, max = 2500),
    list(vec = hit_targets, idx = 4, label = "Hitter RBI target", min = 0, max = 2500),
    list(vec = hit_targets, idx = 5, label = "Hitter AVG target", min = 0.150, max = 0.350),
    list(vec = pit_targets, idx = 1, label = "Pitcher W target", min = 0, max = 200),
    list(vec = pit_targets, idx = 2, label = "Pitcher K target", min = 0, max = 3000),
    list(vec = pit_targets, idx = 3, label = "Pitcher SV target", min = 0, max = 300),
    list(vec = pit_targets, idx = 4, label = "Pitcher ERA target", min = 1.50, max = 10.00),
    list(vec = pit_targets, idx = 5, label = "Pitcher WHIP target", min = 0.80, max = 2.50)
  )

  for (chk in checks) {
    issue <- check_numeric(chk$vec, chk$idx, chk$label, chk$min, chk$max)
    if (!is.null(issue)) {
      issues <- c(issues, issue)
    }
  }

  if (length(issues) > 0) {
    warning(
      paste(
        "Target sanity check flagged possible misalignment/data issues:",
        paste(sprintf("- %s", issues), collapse = "\n"),
        sep = "\n"
      ),
      call. = FALSE
    )
  }
}

normalize_pit_manual <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(df)
  }

  m <- as.matrix(df)
  if (ncol(m) < 11) {
    pad <- matrix("", nrow = nrow(m), ncol = 11 - ncol(m))
    m <- cbind(m, pad)
  } else if (ncol(m) > 11) {
    m <- m[, seq_len(11), drop = FALSE]
  }
  storage.mode(m) <- "character"
  m[is.na(m)] <- ""

  team_col <- trimws(m[, 2])
  adp_col <- suppressWarnings(as.numeric(m[, 3]))
  team_like <- grepl("^[A-Z]{2,3}$", team_col)
  looks_old <- sum(team_like & nzchar(team_col), na.rm = TRUE) >= 1 && sum(!is.na(adp_col), na.rm = TRUE) >= 1

  if (looks_old) {
    out <- matrix("", nrow = nrow(m), ncol = 11)
    out[, 1] <- m[, 1]  # Player
    out[, 2] <- m[, 3]  # ADP
    out[, 3] <- m[, 4]  # IP
    out[, 4] <- m[, 5]  # W
    out[, 5] <- m[, 6]  # K
    out[, 6] <- m[, 7]  # SV
    out[, 7] <- m[, 8]  # ERA
    out[, 8] <- m[, 9]  # WHIP
    out[, 9] <- ""      # spacer
    out[, 10] <- m[, 10] # Round
    out[, 11] <- m[, 11] # Overall Pick
    m <- out
  }

  as.data.frame(m, stringsAsFactors = FALSE)
}

message("Authenticating Google Sheets...")
auth_google_sheets()

all_tabs <- googlesheets4::sheet_names(target_sheet)
if (!projection_tab %in% all_tabs) {
  stop(sprintf("Projection tab '%s' not found in target workbook.", projection_tab))
}

# Preserve manual entries if tab exists.
hit_players <- NULL
hit_pick_data <- NULL
pit_manual <- NULL
hit_targets_existing <- NULL
pit_targets_existing <- NULL
hit_target_defaults <- c("307", "183", "1058", "1032", "0.254")
pit_target_defaults <- c("90", "1368", "67", "3.968", "1.235")

if (tab_name %in% all_tabs) {
  hit_players <- safe_read(target_sheet, tab_name, "B2:B15")

  hit_pick_data <- safe_read(target_sheet, tab_name, "K2:L15")
  if (!has_any_value(hit_pick_data)) {
    hit_pick_data <- safe_read(target_sheet, tab_name, "L2:M15")
  }
  if (!has_any_value(hit_pick_data)) {
    hit_pick_data <- safe_read(target_sheet, tab_name, "O2:P15")
  }

  pit_manual <- safe_read(target_sheet, tab_name, "B25:L33")
  pit_manual <- normalize_pit_manual(pit_manual)

  hit_targets_primary <- safe_read(target_sheet, tab_name, "E18:I18")
  hit_targets_secondary <- safe_read(target_sheet, tab_name, "F18:J18")
  hit_targets_existing <- choose_target_row(hit_targets_primary, hit_targets_secondary, hit_target_defaults)

  pit_targets_primary <- safe_read(target_sheet, tab_name, "E36:I36")
  pit_targets_secondary <- safe_read(target_sheet, tab_name, "F36:J36")
  pit_targets_existing <- choose_target_row(pit_targets_primary, pit_targets_secondary, pit_target_defaults)
} else {
  googlesheets4::sheet_add(target_sheet, tab_name)
}

googlesheets4::sheet_resize(target_sheet, sheet = tab_name, nrow = 300, ncol = 40, exact = FALSE)

hit_targets <- preserve_targets(hit_targets_existing, hit_target_defaults)
pit_targets <- preserve_targets(pit_targets_existing, pit_target_defaults)
flag_implausible_targets(hit_targets, pit_targets)

# Template grid A1:L45.
grid <- matrix("", nrow = 45, ncol = 12)
grid[1, ] <- c(
  "Position", "Player", "ADP", "PA", "HR", "SB", "R", "RBI", "AVG",
  "Dollars.Weights", "Round", "Overall Pick"
)

hitter_slots <- c("C", "C", "1B", "2B", "3B", "SS", "MI", "CI", "OF", "OF", "OF", "OF", "OF", "UT")
grid[2:15, 1] <- hitter_slots

grid[16, 4:10] <- c("PA", "HR", "SB", "R", "RBI", "AVG", "Dollars.Weights")
grid[17, 2] <- "Totals"
grid[18, 2] <- "Targets"
grid[19, 2] <- "Gap (Total-Target)"
grid[20, 2] <- "Remaining Slots"
grid[21, 2] <- "Needed/Slot"
grid[18, 5:9] <- hit_targets

grid[24, 1:12] <- c(
  "Position", "Player", "ADP", "IP", "W", "K", "SV", "ERA", "WHIP", "",
  "Round", "Overall Pick"
)
grid[25:33, 1] <- "P"

grid[34, 4:9] <- c("IP", "W", "K", "SV", "ERA", "WHIP")
grid[35, 2] <- "Totals"
grid[36, 2] <- "Targets"
grid[37, 2] <- "Gap"
grid[38, 2] <- "Remaining Slots"
grid[39, 2] <- "Needed/Slot"
grid[36, 5:9] <- pit_targets

googlesheets4::range_write(
  ss = target_sheet,
  data = as.data.frame(grid, stringsAsFactors = FALSE),
  sheet = tab_name,
  range = "A1:L45",
  col_names = FALSE
)

# Hitter lookup formulas C2:J15.
rows_hit <- 2:15
mk_lookup <- function(r, source_header) {
  sprintf(
    '=IF($B%d="","",IFERROR(XLOOKUP($B%d,INDEX(%s!$A:$ZZ,0,MATCH("player_name",%s!$1:$1,0)),INDEX(%s!$A:$ZZ,0,MATCH("%s",%s!$1:$1,0))),""))',
    r, r, projection_tab, projection_tab, projection_tab, source_header, projection_tab
  )
}

hit_formula_df <- data.frame(
  C = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "adp")),
  D = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "pa")),
  E = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "hr")),
  F = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "sb")),
  G = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "r")),
  H = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "rbi")),
  I = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "avg")),
  J = gs4f(vapply(rows_hit, mk_lookup, character(1), source_header = "dollars_weights")),
  stringsAsFactors = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = hit_formula_df,
  sheet = tab_name,
  range = "C2:J15",
  col_names = FALSE
)

# Hitter summary formulas.
googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    D = gs4f("=SUM(D2:D15)"),
    E = gs4f("=SUM(E2:E15)"),
    F = gs4f("=SUM(F2:F15)"),
    G = gs4f("=SUM(G2:G15)"),
    H = gs4f("=SUM(H2:H15)"),
    I = gs4f("=IFERROR(SUMPRODUCT(D2:D15,I2:I15)/SUM(D2:D15),\"\")"),
    J = gs4f("=SUM(J2:J15)"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "D17:J17",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    E = gs4f("=IF(E18=\"\",\"\",E17-E18)"),
    F = gs4f("=IF(F18=\"\",\"\",F17-F18)"),
    G = gs4f("=IF(G18=\"\",\"\",G17-G18)"),
    H = gs4f("=IF(H18=\"\",\"\",H17-H18)"),
    I = gs4f("=IF(I18=\"\",\"\",I17-I18)"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "E19:I19",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(D = gs4f("=COUNTBLANK(B2:B15)"), stringsAsFactors = FALSE),
  sheet = tab_name,
  range = "D20:D20",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    E = gs4f("=IF($D$20>0,IFERROR((E18-E17)/$D$20,\"\"),\"\")"),
    F = gs4f("=IF($D$20>0,IFERROR((F18-F17)/$D$20,\"\"),\"\")"),
    G = gs4f("=IF($D$20>0,IFERROR((G18-G17)/$D$20,\"\"),\"\")"),
    H = gs4f("=IF($D$20>0,IFERROR((H18-H17)/$D$20,\"\"),\"\")"),
    I = gs4f("=IF($D$20>0,IFERROR((I18-I17)/$D$20,\"\"),\"\")"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "E21:I21",
  col_names = FALSE
)

# Pitcher summary formulas.
googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    D = gs4f("=SUM(D25:D33)"),
    E = gs4f("=SUM(E25:E33)"),
    F = gs4f("=SUM(F25:F33)"),
    G = gs4f("=SUM(G25:G33)"),
    H = gs4f("=IFERROR(SUMPRODUCT(D25:D33,H25:H33)/SUM(D25:D33),\"\")"),
    I = gs4f("=IFERROR(SUMPRODUCT(D25:D33,I25:I33)/SUM(D25:D33),\"\")"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "D35:I35",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    E = gs4f("=IF(E36=\"\",\"\",E35-E36)"),
    F = gs4f("=IF(F36=\"\",\"\",F35-F36)"),
    G = gs4f("=IF(G36=\"\",\"\",G35-G36)"),
    H = gs4f("=IF(H36=\"\",\"\",H36-H35)"),
    I = gs4f("=IF(I36=\"\",\"\",I36-I35)"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "E37:I37",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(D = gs4f("=COUNTBLANK(B25:B33)"), stringsAsFactors = FALSE),
  sheet = tab_name,
  range = "D38:D38",
  col_names = FALSE
)

googlesheets4::range_write(
  ss = target_sheet,
  data = data.frame(
    E = gs4f("=IF($D$38>0,IFERROR((E36-E35)/$D$38,\"\"),\"\")"),
    F = gs4f("=IF($D$38>0,IFERROR((F36-F35)/$D$38,\"\"),\"\")"),
    G = gs4f("=IF($D$38>0,IFERROR((G36-G35)/$D$38,\"\"),\"\")"),
    stringsAsFactors = FALSE
  ),
  sheet = tab_name,
  range = "E39:G39",
  col_names = FALSE
)

# Restore preserved manual entries.
if (!is.null(hit_players) && nrow(hit_players) > 0) {
  googlesheets4::range_write(target_sheet, hit_players, sheet = tab_name, range = "B2:B15", col_names = FALSE)
}
if (!is.null(hit_pick_data) && nrow(hit_pick_data) > 0) {
  googlesheets4::range_write(target_sheet, hit_pick_data, sheet = tab_name, range = "K2:L15", col_names = FALSE)
}
if (!is.null(pit_manual) && nrow(pit_manual) > 0) {
  googlesheets4::range_write(target_sheet, pit_manual, sheet = tab_name, range = "B25:L33", col_names = FALSE)
}

message(sprintf("Tab '%s' created/updated.", tab_name))
