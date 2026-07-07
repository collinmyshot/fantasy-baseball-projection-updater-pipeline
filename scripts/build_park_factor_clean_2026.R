#!/usr/bin/env Rscript
source(file.path("R", "utils.R"))

# iPF formulation (July 2026): Overall is the main model's wOBAcon park effect
# taken directly; BACON, HR, and Carry are component lenses. The former Google
# Sheets export is retired — the fbb-tools Shiny app is the only publish path.
parsed <- parse_cli_args(list(
  output_dir     = list(flag = "--output-dir",     default = file.path("data", "processed", "park_factors")),
  events_csv     = list(flag = "--events-csv",     default = file.path("data", "manual", "park_era_events.csv")),
  home_parks_csv = list(flag = "--home-parks-csv", default = file.path("data", "manual", "mlb_home_parks_2026_verified.csv")),
  season_target  = list(flag = "--season-target",  default = 2026, type = "numeric")
))

output_dir     <- parsed$output_dir
events_csv     <- parsed$events_csv
home_parks_csv <- parsed$home_parks_csv
season_target  <- as.integer(parsed$season_target)

input_path <- file.path(output_dir, "park_factors_savant_style_with_id.csv")
if (!file.exists(input_path)) {
  stop(sprintf("Missing input file: %s", input_path))
}

if (!file.exists(events_csv)) {
  stop(sprintf("Missing events CSV: %s", events_csv))
}
if (!file.exists(home_parks_csv)) {
  stop(sprintf("Missing verified home park mapping CSV: %s", home_parks_csv))
}

normalize_team <- function(x) {
  x <- toupper(trimws(as.character(x)))
  mapped <- c(KC = "KCR", ARI = "AZ", TB = "TBR", OAK = "ATH", WAS = "WSH")
  hit <- x %in% names(mapped)
  x[hit] <- mapped[x[hit]]
  x
}

park_team_token <- function(team_id) {
  t <- toupper(as.character(team_id))
  if (t == "KC") return("kcr")
  tolower(t)
}

suffix_from_id <- function(park_era_id) {
  sub("^.*__", "", as.character(park_era_id))
}

extract_year_end <- function(years_used) {
  y <- as.character(years_used)
  out <- suppressWarnings(as.integer(sub("^.*-", "", y)))
  single <- suppressWarnings(as.integer(y))
  out[is.na(out)] <- single[is.na(out)]
  out
}

extract_year_start <- function(years_used) {
  y <- as.character(years_used)
  out <- suppressWarnings(as.integer(sub("-.*$", "", y)))
  out
}

events <- utils::read.csv(events_csv, stringsAsFactors = FALSE, check.names = FALSE)
events$team <- normalize_team(events$team)
events$start_date <- suppressWarnings(as.Date(events$start_date))
events$end_date <- suppressWarnings(as.Date(events$end_date))
events$era_suffix <- tolower(trimws(as.character(events$era_suffix)))

target_start <- as.Date(sprintf("%d-01-01", season_target))
target_end <- as.Date(sprintf("%d-12-31", season_target))
active_events <- events[
  !is.na(events$start_date) &
    events$start_date <= target_end &
    (is.na(events$end_date) | events$end_date >= target_start),
]

active_events <- active_events[order(active_events$team, active_events$start_date, na.last = TRUE), ]

active_suffix <- do.call(rbind, lapply(split(active_events, active_events$team), function(df) {
  df <- df[order(df$start_date, decreasing = TRUE), ]
  data.frame(
    team_norm = df$team[1],
    target_suffix = df$era_suffix[1],
    stringsAsFactors = FALSE
  )
}))

display <- utils::read.csv(input_path, stringsAsFactors = FALSE, check.names = FALSE)
display$team_token <- vapply(display$team_id, park_team_token, character(1))
display$team_norm <- normalize_team(display$team_id)
display$park_suffix <- suffix_from_id(display$park_era_id)
display$year_end <- extract_year_end(display$years_used)
display$year_start <- extract_year_start(display$years_used)

team_ids <- sort(unique(display$team_id))
verified <- utils::read.csv(home_parks_csv, stringsAsFactors = FALSE, check.names = FALSE)
verified$team_abbr <- toupper(trimws(as.character(verified$team_abbr)))
verified$team_norm <- normalize_team(verified$team_abbr)
verified$venue_name <- trimws(as.character(verified$venue_name))

fuzzy_match_venue <- function(rows, venue_name) {
  v <- tolower(trimws(as.character(venue_name)))
  rp <- tolower(trimws(as.character(rows$park)))

  exact <- rows[rp == v, , drop = FALSE]
  if (nrow(exact) > 0) {
    return(exact)
  }

  alnum <- function(x) gsub("[^a-z0-9]", "", x)
  v2 <- alnum(v)
  rp2 <- alnum(rp)
  rough <- rows[rp2 == v2, , drop = FALSE]
  if (nrow(rough) > 0) {
    return(rough)
  }

  # Relax matching for common park naming drift.
  key_contains <- function(patterns) {
    hit <- rep(TRUE, nrow(rows))
    for (p in patterns) {
      hit <- hit & grepl(p, rp, fixed = TRUE)
    }
    rows[hit, , drop = FALSE]
  }

  if (grepl("tropicana", v, fixed = TRUE)) {
    m <- key_contains(c("tropicana"))
    if (nrow(m) > 0) return(m)
  }
  if (grepl("kauffman", v, fixed = TRUE)) {
    m <- key_contains(c("kauffman"))
    if (nrow(m) > 0) return(m)
  }
  if (grepl("sutter", v, fixed = TRUE)) {
    m <- key_contains(c("sutter"))
    if (nrow(m) > 0) return(m)
  }
  if (grepl("globe life field", v, fixed = TRUE)) {
    m <- key_contains(c("globe", "field"))
    if (nrow(m) > 0) return(m)
  }
  if (grepl("rate", v, fixed = TRUE)) {
    m <- key_contains(c("rate"))
    if (nrow(m) > 0) return(m)
  }

  rows[0, , drop = FALSE]
}

pick_row_for_team <- function(team_id, table_data) {
  rows <- table_data[table_data$team_id == team_id, ]
  rows <- rows[order(rows$year_end, rows$year_start, rows$total_bbe, decreasing = TRUE), ]
  default_row <- rows[1, , drop = FALSE]

  team_norm <- normalize_team(team_id)
  suffix_row <- active_suffix[active_suffix$team_norm == team_norm, , drop = FALSE]
  if (nrow(suffix_row) == 0) {
    # No known future event; if verified 2026 venue is available, match to it.
    vm <- verified[verified$team_norm == team_norm, , drop = FALSE]
    if (nrow(vm) > 0 && nzchar(vm$venue_name[1])) {
      venue_rows <- fuzzy_match_venue(rows, vm$venue_name[1])
      if (nrow(venue_rows) > 0) {
        venue_rows <- venue_rows[order(venue_rows$year_end, venue_rows$total_bbe, decreasing = TRUE), ]
        return(venue_rows[1, , drop = FALSE])
      }
    }
    return(default_row)
  }

  target_suffix <- suffix_row$target_suffix[1]
  exact <- rows[rows$park_suffix == target_suffix, , drop = FALSE]
  if (nrow(exact) > 0) {
    exact <- exact[order(exact$year_end, exact$total_bbe, decreasing = TRUE), ]
    return(exact[1, , drop = FALSE])
  }

  # 2026 return-home case for TB: use Trop baseline if future-era row does not exist yet.
  if (team_norm == "TBR" && target_suffix == "trop_return") {
    trop <- rows[rows$park_suffix == "base", , drop = FALSE]
    if (nrow(trop) > 0) {
      trop <- trop[order(trop$year_end, trop$total_bbe, decreasing = TRUE), ]
      return(trop[1, , drop = FALSE])
    }
  }

  # For future changed dimensions (e.g., KCR walls_in) without observed data yet,
  # fallback to verified 2026 stadium row if present; else most recent observed row.
  vm <- verified[verified$team_norm == team_norm, , drop = FALSE]
  if (nrow(vm) > 0 && nzchar(vm$venue_name[1])) {
    venue_rows <- fuzzy_match_venue(rows, vm$venue_name[1])
    if (nrow(venue_rows) > 0) {
      venue_rows <- venue_rows[order(venue_rows$year_end, venue_rows$total_bbe, decreasing = TRUE), ]
      return(venue_rows[1, , drop = FALSE])
    }
  }
  default_row
}

# Upstream index columns are standardized over ALL park eras, including
# retired ones (Turner Field, Walltimore, COVID venues...). Re-standardize the
# raw deltas over the 30 selected rows so that 100 means "average current
# park" in every displayed table.
restandardize_over_selected <- function(picked_tbl) {
  restd <- function(x) {
    x <- suppressWarnings(as.numeric(x))
    if (all(!is.finite(x))) {
      return(rep(NA_real_, length(x)))
    }
    std_index(x)
  }
  picked_tbl$bacon_idx_100 <- restd(picked_tbl$bacon_resid)
  picked_tbl$hr_idx_100 <- restd(picked_tbl$hr_resid)
  picked_tbl$xbh_idx_100 <- restd(picked_tbl$xbh_resid)
  if ("carry_ft" %in% names(picked_tbl)) {
    picked_tbl$carry_idx_100 <- restd(picked_tbl$carry_ft)
  }
  picked_tbl$overall_pf_idx_100 <- restd(picked_tbl$overall_resid)
  picked_tbl
}

pick_display_rows <- function(table_data) {
  ids <- sort(unique(table_data$team_id))
  picked <- do.call(rbind, lapply(ids, function(id) pick_row_for_team(id, table_data)))
  rownames(picked) <- NULL
  picked <- picked[!duplicated(picked$team_id), , drop = FALSE]
  picked <- restandardize_over_selected(picked)
  picked <- picked[order(picked$overall_pf_idx_100, decreasing = TRUE, na.last = TRUE), ]
  picked$Rank <- seq_len(nrow(picked))
  picked
}

picked <- pick_display_rows(display)

round2 <- function(x) round(as.numeric(x), 2)
fmt2 <- function(x) sprintf("%.2f", round2(x))

clean_from_picked <- function(picked_tbl) {
  col_or_na <- function(nm) {
    if (nm %in% names(picked_tbl)) round2(picked_tbl[[nm]]) else rep(NA_real_, nrow(picked_tbl))
  }
  data.frame(
    Rank = as.integer(picked_tbl$Rank),
    Team = as.character(picked_tbl$team),
    Park = as.character(picked_tbl$park),
    Years = as.character(picked_tbl$years_used),
    `Overall Park Factor` = round2(picked_tbl$overall_pf_idx_100),
    `BACON Park Factor` = round2(picked_tbl$bacon_idx_100),
    `HR Park Factor` = round2(picked_tbl$hr_idx_100),
    `Carry Park Factor` = col_or_na("carry_idx_100"),
    `Carry (ft vs avg)` = col_or_na("carry_ft"),
    `Total BBE` = as.integer(round(as.numeric(picked_tbl$total_bbe))),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

build_known_park_effects <- function(overall_tbl, half_1h_tbl, half_2h_tbl, top_n = 12L) {
  top_n <- as.integer(max(1L, top_n))
  overall_meta <- overall_tbl[, c("team_id", "team", "park", "years_used", "total_bbe"), drop = FALSE]
  overall_meta <- overall_meta[!duplicated(overall_meta$team_id), , drop = FALSE]

  ov <- overall_tbl
  ov$delta_hr_bacon <- suppressWarnings(as.numeric(ov$hr_idx_100) - as.numeric(ov$bacon_idx_100))
  ov$abs_delta_hr_bacon <- abs(ov$delta_hr_bacon)
  ov$effect_group <- ifelse(ov$delta_hr_bacon >= 0, "HR-leaning", "BACON-leaning")
  ov$group_order <- ifelse(ov$effect_group == "HR-leaning", 1L, 2L)
  per_group_n <- as.integer(max(1L, ceiling(top_n / 2)))
  ov <- ov[order(ov$group_order, -ov$abs_delta_hr_bacon, na.last = TRUE), ]
  ov <- do.call(rbind, lapply(split(ov, ov$effect_group), function(dg) {
    dg <- dg[order(dg$group_order, -dg$abs_delta_hr_bacon, na.last = TRUE), , drop = FALSE]
    dg[seq_len(min(per_group_n, nrow(dg))), , drop = FALSE]
  }))
  ov <- ov[order(ov$group_order, -ov$abs_delta_hr_bacon, na.last = TRUE), , drop = FALSE]

  t1 <- data.frame(
    Rank = seq_len(nrow(ov)),
    Team = as.character(ov$team),
    Park = as.character(ov$park),
    Years = as.character(ov$years_used),
    `BACON Park Factor` = round2(ov$bacon_idx_100),
    `HR Park Factor` = round2(ov$hr_idx_100),
    Difference = round2(ov$delta_hr_bacon),
    `Abs Difference` = round2(ov$abs_delta_hr_bacon),
    `Total BBE` = as.integer(round(as.numeric(ov$total_bbe))),
    Notes = ifelse(ov$delta_hr_bacon >= 0, "HR-leaning park profile", "BACON-leaning park profile"),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  h1 <- half_1h_tbl[, c("team_id", "team", "park", "years_used", "overall_pf_idx_100"), drop = FALSE]
  h2 <- half_2h_tbl[, c("team_id", "team", "park", "years_used", "overall_pf_idx_100"), drop = FALSE]
  names(h1)[names(h1) == "overall_pf_idx_100"] <- "pf_1h"
  names(h2)[names(h2) == "overall_pf_idx_100"] <- "pf_2h"

  hs <- merge(h1, h2, by = "team_id", suffixes = c("_1h", "_2h"), all = FALSE)
  hs$team <- ifelse(nzchar(as.character(hs$team_1h)), hs$team_1h, hs$team_2h)
  hs$park <- ifelse(nzchar(as.character(hs$park_2h)), hs$park_2h, hs$park_1h)
  hs$years <- ifelse(nzchar(as.character(hs$years_used_2h)), hs$years_used_2h, hs$years_used_1h)
  hs$delta_2h_1h <- suppressWarnings(as.numeric(hs$pf_2h) - as.numeric(hs$pf_1h))
  hs$abs_delta <- abs(hs$delta_2h_1h)
  hs$effect_group <- ifelse(hs$delta_2h_1h >= 0, "2H-hitter", "2H-pitcher")
  hs$group_order <- ifelse(hs$effect_group == "2H-hitter", 1L, 2L)
  hs <- hs[order(hs$group_order, -hs$abs_delta, na.last = TRUE), ]
  hs <- do.call(rbind, lapply(split(hs, hs$effect_group), function(dg) {
    dg <- dg[order(dg$group_order, -dg$abs_delta, na.last = TRUE), , drop = FALSE]
    dg[seq_len(min(per_group_n, nrow(dg))), , drop = FALSE]
  }))
  hs <- hs[order(hs$group_order, -hs$abs_delta, na.last = TRUE), , drop = FALSE]

  hs <- merge(hs, overall_meta, by = "team_id", all.x = TRUE)
  hs$team <- ifelse(nzchar(as.character(hs$team.y)), hs$team.y, hs$team.x)
  hs$park <- hs$park.y
  hs$years <- hs$years_used
  hs <- hs[order(hs$group_order, -hs$abs_delta, na.last = TRUE), , drop = FALSE]

  t2 <- data.frame(
    Rank = seq_len(nrow(hs)),
    Team = as.character(hs$team),
    Park = as.character(hs$park),
    Years = as.character(hs$years),
    `BACON Park Factor` = round2(hs$pf_1h),
    `HR Park Factor` = round2(hs$pf_2h),
    Difference = round2(hs$delta_2h_1h),
    `Abs Difference` = round2(hs$abs_delta),
    `Total BBE` = as.integer(round(as.numeric(hs$total_bbe))),
    Notes = ifelse(hs$delta_2h_1h >= 0, "More hitter-friendly in 2H", "More pitcher-friendly in 2H"),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  combined <- rbind(
    cbind(Analysis = "HR vs BACON Gap", t1, stringsAsFactors = FALSE),
    cbind(Analysis = "1H vs 2H Overall PF Gap", t2, stringsAsFactors = FALSE)
  )
  rownames(combined) <- NULL

  template <- as.data.frame(
    as.list(stats::setNames(rep(NA, ncol(t1)), names(t1))),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
  blank_row <- template

  title_1 <- template
  title_1$Team <- "Table 1: HR vs BACON Park Factor Gaps"
  title_1$Park <- "BACON Park Factor vs HR Park Factor"

  subtitle_1 <- template
  subtitle_1$Team <- "Positive Difference favors HR environment; negative favors BACON environment"
  subheader_1 <- template
  subheader_1$Rank <- "Rank"
  subheader_1$Team <- "Team"
  subheader_1$Park <- "Park"
  subheader_1$Years <- "Years"
  subheader_1$`BACON Park Factor` <- "BACON Park Factor"
  subheader_1$`HR Park Factor` <- "HR Park Factor"
  subheader_1$Difference <- "Difference"
  subheader_1$`Abs Difference` <- "Abs Difference"
  subheader_1$`Total BBE` <- "Total BBE"
  subheader_1$Notes <- "Notes"

  title_2 <- template
  title_2$Team <- "Table 2: 1H vs 2H Overall PF Gaps"
  title_2$Park <- "1H Overall PF vs 2H Overall PF"

  subtitle_2 <- template
  subtitle_2$Team <- "Positive Difference = more hitter-friendly in 2H"
  subheader_2 <- template
  subheader_2$Rank <- "Rank"
  subheader_2$Team <- "Team"
  subheader_2$Park <- "Park"
  subheader_2$Years <- "Years"
  subheader_2$`BACON Park Factor` <- "1H Overall PF"
  subheader_2$`HR Park Factor` <- "2H Overall PF"
  subheader_2$Difference <- "Difference"
  subheader_2$`Abs Difference` <- "Abs Difference"
  subheader_2$`Total BBE` <- "Total BBE"
  subheader_2$Notes <- "Notes"

  layout <- rbind(
    title_1, subtitle_1, subheader_1, t1, blank_row,
    title_2, subtitle_2, subheader_2, t2
  )
  rownames(layout) <- NULL

  list(
    combined = combined,
    hr_bacon = t1,
    half_split = t2,
    layout = layout
  )
}

build_half_display <- function(half_label) {
  main_path <- file.path(output_dir, "park_factors_by_half.csv")
  bacon_path <- file.path(output_dir, "park_factors_bacon_by_half.csv")
  hr_path <- file.path(output_dir, "park_factors_hr_by_half.csv")
  xbh_path <- file.path(output_dir, "park_factors_xbh_by_half.csv")
  for (pth in c(main_path, bacon_path, hr_path, xbh_path)) {
    if (!file.exists(pth)) {
      stop(sprintf("Missing required half file: %s", pth))
    }
  }

  read_comp <- function(path, comp_label, delta_col = "delta_half") {
    d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
    d <- d[d$half == half_label, ]
    out <- d[, c("park_era_id", "home_team", "n_bbe", delta_col, "park_se"), drop = FALSE]
    names(out)[names(out) == delta_col] <- paste0(comp_label, "_delta")
    names(out)[names(out) == "park_se"] <- paste0(comp_label, "_se")
    names(out)[names(out) == "n_bbe"] <- paste0(comp_label, "_n_bbe")
    out
  }

  # Overall at the half level comes straight from the main model's park-half
  # estimates (park effect + park-half deviation), not from a component blend.
  m <- read_comp(main_path, "overall", delta_col = "delta_woba_over_xwoba_half")
  b <- read_comp(bacon_path, "bacon")
  h <- read_comp(hr_path, "hr")
  x <- read_comp(xbh_path, "xbh")
  merged <- merge(m, b, by = c("park_era_id", "home_team"), all = TRUE)
  merged <- merge(merged, h, by = c("park_era_id", "home_team"), all = TRUE)
  merged <- merge(merged, x, by = c("park_era_id", "home_team"), all = TRUE)

  carry_path <- file.path(output_dir, "park_factors_distance_by_half.csv")
  if (file.exists(carry_path)) {
    merged <- merge(merged, read_comp(carry_path, "carry"), by = c("park_era_id", "home_team"), all.x = TRUE)
  } else {
    merged$carry_delta <- NA_real_
  }

  era_lookup <- unique(display[, c(
    "park_era_id", "team_id", "team", "park", "years_used",
    "team_token", "team_norm", "park_suffix", "year_end", "year_start"
  )])
  merged <- merge(merged, era_lookup, by = "park_era_id", all.x = TRUE)

  merged$total_bbe <- suppressWarnings(as.numeric(merged$overall_n_bbe))
  merged$overall_resid <- suppressWarnings(as.numeric(merged$overall_delta))
  merged$bacon_resid <- suppressWarnings(as.numeric(merged$bacon_delta))
  merged$hr_resid <- suppressWarnings(as.numeric(merged$hr_delta))
  merged$xbh_resid <- suppressWarnings(as.numeric(merged$xbh_delta))
  merged$carry_ft <- suppressWarnings(as.numeric(merged$carry_delta))

  merged$bacon_idx_100 <- std_index(merged$bacon_resid)
  merged$hr_idx_100 <- std_index(merged$hr_resid)
  merged$xbh_idx_100 <- std_index(merged$xbh_resid)
  merged$carry_idx_100 <- std_index(merged$carry_ft)
  merged$overall_pf_idx_100 <- std_index(merged$overall_resid)

  merged <- merged[!is.na(merged$team_id), , drop = FALSE]
  merged
}

# Batter-side slices: same shape as the half slices, sourced from the by_hand
# tables (park effect + park-hand deviation, shrunken toward the park's
# overall number where one side's sample is thin).
build_hand_display <- function(hand_label) {
  main_path <- file.path(output_dir, "park_factors_by_hand.csv")
  bacon_path <- file.path(output_dir, "park_factors_bacon_by_hand.csv")
  hr_path <- file.path(output_dir, "park_factors_hr_by_hand.csv")
  xbh_path <- file.path(output_dir, "park_factors_xbh_by_hand.csv")
  for (pth in c(main_path, bacon_path, hr_path, xbh_path)) {
    if (!file.exists(pth)) {
      stop(sprintf("Missing required hand file: %s", pth))
    }
  }

  read_hand <- function(path, comp_label) {
    d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
    d <- d[d$hand == hand_label, ]
    out <- d[, c("park_era_id", "home_team", "n_bbe", "delta_hand", "hand_se_combined"), drop = FALSE]
    names(out)[names(out) == "delta_hand"] <- paste0(comp_label, "_delta")
    names(out)[names(out) == "hand_se_combined"] <- paste0(comp_label, "_se")
    names(out)[names(out) == "n_bbe"] <- paste0(comp_label, "_n_bbe")
    out
  }

  m <- read_hand(main_path, "overall")
  b <- read_hand(bacon_path, "bacon")
  h <- read_hand(hr_path, "hr")
  x <- read_hand(xbh_path, "xbh")
  merged <- merge(m, b, by = c("park_era_id", "home_team"), all = TRUE)
  merged <- merge(merged, h, by = c("park_era_id", "home_team"), all = TRUE)
  merged <- merge(merged, x, by = c("park_era_id", "home_team"), all = TRUE)

  carry_path <- file.path(output_dir, "park_factors_distance_by_hand.csv")
  if (file.exists(carry_path)) {
    merged <- merge(merged, read_hand(carry_path, "carry"), by = c("park_era_id", "home_team"), all.x = TRUE)
  } else {
    merged$carry_delta <- NA_real_
  }

  era_lookup <- unique(display[, c(
    "park_era_id", "team_id", "team", "park", "years_used",
    "team_token", "team_norm", "park_suffix", "year_end", "year_start"
  )])
  merged <- merge(merged, era_lookup, by = "park_era_id", all.x = TRUE)

  merged$total_bbe <- suppressWarnings(as.numeric(merged$overall_n_bbe))
  merged$overall_resid <- suppressWarnings(as.numeric(merged$overall_delta))
  merged$bacon_resid <- suppressWarnings(as.numeric(merged$bacon_delta))
  merged$hr_resid <- suppressWarnings(as.numeric(merged$hr_delta))
  merged$xbh_resid <- suppressWarnings(as.numeric(merged$xbh_delta))
  merged$carry_ft <- suppressWarnings(as.numeric(merged$carry_delta))

  merged$bacon_idx_100 <- std_index(merged$bacon_resid)
  merged$hr_idx_100 <- std_index(merged$hr_resid)
  merged$xbh_idx_100 <- std_index(merged$xbh_resid)
  merged$carry_idx_100 <- std_index(merged$carry_ft)
  merged$overall_pf_idx_100 <- std_index(merged$overall_resid)

  merged <- merged[!is.na(merged$team_id), , drop = FALSE]
  merged
}

clean <- clean_from_picked(picked)
display_1h <- build_half_display("1H")
display_2h <- build_half_display("2H")
picked_1h <- pick_display_rows(display_1h)
picked_2h <- pick_display_rows(display_2h)
clean_1h <- clean_from_picked(picked_1h)
clean_2h <- clean_from_picked(picked_2h)
display_lhb <- build_hand_display("L")
display_rhb <- build_hand_display("R")
picked_lhb <- pick_display_rows(display_lhb)
picked_rhb <- pick_display_rows(display_rhb)
clean_lhb <- clean_from_picked(picked_lhb)
clean_rhb <- clean_from_picked(picked_rhb)
known_effects <- build_known_park_effects(picked, picked_1h, picked_2h, top_n = 12L)

utils::write.csv(
  clean,
  file.path(output_dir, "park_factors_savant_style_clean_2026.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  picked,
  file.path(output_dir, "park_factors_savant_style_clean_2026_with_id.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  clean_1h,
  file.path(output_dir, "park_factors_savant_style_clean_2026_1H.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  clean_2h,
  file.path(output_dir, "park_factors_savant_style_clean_2026_2H.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  known_effects$combined,
  file.path(output_dir, "park_factor_known_effects_2026.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  picked_1h,
  file.path(output_dir, "park_factors_savant_style_clean_2026_1H_with_id.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  picked_2h,
  file.path(output_dir, "park_factors_savant_style_clean_2026_2H_with_id.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  clean_lhb,
  file.path(output_dir, "park_factors_savant_style_clean_2026_LHB.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  clean_rhb,
  file.path(output_dir, "park_factors_savant_style_clean_2026_RHB.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  picked_lhb,
  file.path(output_dir, "park_factors_savant_style_clean_2026_LHB_with_id.csv"),
  row.names = FALSE,
  na = ""
)

utils::write.csv(
  picked_rhb,
  file.path(output_dir, "park_factors_savant_style_clean_2026_RHB_with_id.csv"),
  row.names = FALSE,
  na = ""
)

message("Wrote clean 2026 park factor table: ", file.path(output_dir, "park_factors_savant_style_clean_2026.csv"))
message("Publish path (single source of truth): copy the FIVE _with_id CSVs to fbb-tools-repo/data/park_factors/ , then deploy the Shiny app:")
message("  - park_factors_savant_style_clean_2026_with_id.csv")
message("  - park_factors_savant_style_clean_2026_1H_with_id.csv")
message("  - park_factors_savant_style_clean_2026_2H_with_id.csv")
message("  - park_factors_savant_style_clean_2026_LHB_with_id.csv")
message("  - park_factors_savant_style_clean_2026_RHB_with_id.csv")
message("  Both the PF leaderboard and the SP Streamonator/Outlook read these _with_id files. The non _with_id display CSVs are monorepo-only (article/comparison inputs); fbb-tools no longer uses them.")
