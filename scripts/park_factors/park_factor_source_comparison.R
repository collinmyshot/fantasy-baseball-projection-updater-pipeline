#!/usr/bin/env Rscript
# Park Factor Source Comparison: Ours vs Savant vs FanGraphs vs Baseball Reference
# Compares: (1) correlation with runs scored, (2) year-over-year stability

library(dplyr)
library(tidyr)

proj_root <- "/Users/ckaufman/Documents/New project"

# ─── 1. Load all park factor sources ─────────────────────────────────────────

## Our custom PFs: validation_detail has per-season predicted effects by park_era
## We need to aggregate halves → season-level, and map park_era → team
our_detail <- read.csv(file.path(proj_root, "data/processed/park_factors/validation_detail.csv"))

# Map park_era_id to team using overall file
our_overall <- read.csv(file.path(proj_root, "data/processed/park_factors/park_factors_overall.csv"))
park_era_team <- our_overall %>%
  select(park_era_id, home_team) %>%
  distinct()

# Aggregate our PFs: weighted mean of predicted effect across 1H/2H, by park_era × season
our_pf <- our_detail %>%
  filter(season >= 2018, season <= 2025, season != 2020) %>%
  group_by(park_era_id, season) %>%
  summarize(
    pred_effect = weighted.mean(pred_effect, n),
    realized_effect = weighted.mean(realized, n),
    total_n = sum(n),
    .groups = "drop"
  ) %>%
  left_join(park_era_team, by = "park_era_id") %>%
  filter(!is.na(home_team)) %>%
  # For teams with multiple park_eras in a season, take the one with most BBEs
  group_by(home_team, season) %>%
  slice_max(total_n, n = 1) %>%
  ungroup() %>%
  mutate(
    our_pf = (1 + pred_effect) * 100,
    our_realized = (1 + realized_effect) * 100
  ) %>%
  select(team = home_team, season, our_pf, our_realized, total_n)

cat("=== Our PFs ===\n")
cat("Seasons:", paste(sort(unique(our_pf$season)), collapse = ", "), "\n")
cat("Teams per season:\n")
print(our_pf %>% count(season))

## Savant PFs (2015-2024)
savant <- readRDS(file.path(proj_root, "data/processed/streamonator_weight_analysis/savant_pf_2015_2024.rds")) %>%
  filter(season >= 2015, season <= 2025, season != 2020) %>%
  select(season, team_display = name_display_club, savant_runs = index_runs,
         savant_hr = index_hr, savant_woba = index_woba)

cat("\n=== Savant PFs ===\n")
cat("Seasons:", paste(sort(unique(savant$season)), collapse = ", "), "\n")

## FanGraphs PFs
fg <- read.csv(file.path(proj_root, "data/raw/fg_park_factors_by_year.csv")) %>%
  filter(season >= 2015, season <= 2025, season != 2020) %>%
  select(season, fg_team = team, fg_overall = pf_overall, fg_1yr = pf_1yr, fg_hr = pf_hr)

cat("\n=== FG PFs ===\n")
cat("Seasons:", paste(sort(unique(fg$season)), collapse = ", "), "\n")

## Baseball Reference PFs
bref <- read.csv(file.path(proj_root, "data/raw/bref_park_factors_by_year.csv")) %>%
  filter(season >= 2015, season <= 2025, season != 2020) %>%
  select(season, bref_team = team, bref_pf = pf_batting)

cat("\n=== BRef PFs ===\n")
cat("Seasons:", paste(sort(unique(bref$season)), collapse = ", "), "\n")

# ─── 2. Team name normalization ──────────────────────────────────────────────

# Create a canonical team abbreviation mapping
# Our data uses abbreviations (COL, NYY, etc)
# Savant uses display names (Rockies, Yankees, etc)
# FG uses short names (Rockies, Yankees, etc)
# BRef uses full names (Colorado Rockies, New York Yankees, etc)

team_map <- tribble(
  ~abbrev, ~savant_name, ~fg_name, ~bref_name,
  "ARI", "D-backs", "Diamondbacks", "Arizona Diamondbacks",
  "ATL", "Braves", "Braves", "Atlanta Braves",
  "BAL", "Orioles", "Orioles", "Baltimore Orioles",
  "BOS", "Red Sox", "Red Sox", "Boston Red Sox",
  "CHC", "Cubs", "Cubs", "Chicago Cubs",
  "CWS", "White Sox", "White Sox", "Chicago White Sox",
  "CIN", "Reds", "Reds", "Cincinnati Reds",
  "CLE", "Guardians", "Guardians", "Cleveland Guardians",
  "COL", "Rockies", "Rockies", "Colorado Rockies",
  "DET", "Tigers", "Tigers", "Detroit Tigers",
  "HOU", "Astros", "Astros", "Houston Astros",
  "KCR", "Royals", "Royals", "Kansas City Royals",
  "LAA", "Angels", "Angels", "Los Angeles Angels",  # BRef shows "Los Angeles Angels of Anaheim" pre-2021, normalized above
  "LAD", "Dodgers", "Dodgers", "Los Angeles Dodgers",
  "MIA", "Marlins", "Marlins", "Miami Marlins",
  "MIL", "Brewers", "Brewers", "Milwaukee Brewers",
  "MIN", "Twins", "Twins", "Minnesota Twins",
  "NYM", "Mets", "Mets", "New York Mets",
  "NYY", "Yankees", "Yankees", "New York Yankees",
  "ATH", "Athletics", "Athletics", "Oakland Athletics",
  # Historical name variations handled below
  "PHI", "Phillies", "Phillies", "Philadelphia Phillies",
  "PIT", "Pirates", "Pirates", "Pittsburgh Pirates",
  "SDP", "Padres", "Padres", "San Diego Padres",
  "SFG", "Giants", "Giants", "San Francisco Giants",
  "SEA", "Mariners", "Mariners", "Seattle Mariners",
  "STL", "Cardinals", "Cardinals", "St. Louis Cardinals",
  "TBR", "Rays", "Rays", "Tampa Bay Rays",
  "TEX", "Rangers", "Rangers", "Texas Rangers",
  "TOR", "Blue Jays", "Blue Jays", "Toronto Blue Jays",
  "WSN", "Nationals", "Nationals", "Washington Nationals"
)

# Handle Cleveland name changes
cle_savant_old <- c("Indians")
cle_fg_old <- c("Indians", "Cleveland")
cle_bref_old <- c("Cleveland Indians")

savant <- savant %>%
  mutate(team_display = ifelse(team_display %in% cle_savant_old, "Guardians", team_display))
fg <- fg %>%
  mutate(fg_team = ifelse(fg_team %in% cle_fg_old, "Guardians", fg_team))
bref <- bref %>%
  mutate(bref_team = ifelse(bref_team %in% cle_bref_old, "Cleveland Guardians", bref_team)) %>%
  mutate(bref_team = case_when(
    bref_team == "Los Angeles Angels of Anaheim" ~ "Los Angeles Angels",
    bref_team == "Florida Marlins" ~ "Miami Marlins",
    TRUE ~ bref_team
  ))

# Also need to handle Athletics → Oakland Athletics for bref
# (They may show differently in older years)
# Map any remaining using fuzzy partial match on "Athletics"
bref <- bref %>%
  mutate(bref_team = ifelse(grepl("Athletics", bref_team) & bref_team != "Oakland Athletics",
                            "Oakland Athletics", bref_team))

# Also handle Angels name variations
savant <- savant %>%
  mutate(team_display = ifelse(team_display == "Los Angeles Angels of Anaheim", "Angels", team_display))

# Join everything to canonical abbreviation
savant_j <- savant %>%
  left_join(team_map %>% select(abbrev, savant_name), by = c("team_display" = "savant_name")) %>%
  select(season, team = abbrev, savant_runs, savant_hr, savant_woba)

fg_j <- fg %>%
  left_join(team_map %>% select(abbrev, fg_name), by = c("fg_team" = "fg_name")) %>%
  select(season, team = abbrev, fg_overall, fg_1yr, fg_hr)

bref_j <- bref %>%
  left_join(team_map %>% select(abbrev, bref_name), by = c("bref_team" = "bref_name")) %>%
  select(season, team = abbrev, bref_pf)

# Check for NA matches
cat("\n=== Join diagnostics ===\n")
cat("Savant unmatched:", sum(is.na(savant_j$team)), "\n")
cat("FG unmatched:", sum(is.na(fg_j$team)), "\n")
cat("BRef unmatched:", sum(is.na(bref_j$team)), "\n")

if (sum(is.na(savant_j$team)) > 0) {
  cat("Savant unmatched names:", unique(savant$team_display[is.na(savant_j$team)]), "\n")
}
if (sum(is.na(fg_j$team)) > 0) {
  cat("FG unmatched names:", unique(fg$fg_team[is.na(fg_j$team)]), "\n")
}
if (sum(is.na(bref_j$team)) > 0) {
  cat("BRef unmatched names:", unique(bref$bref_team[is.na(bref_j$team)]), "\n")
}

# ─── 3. Merge all sources ────────────────────────────────────────────────────

# Start with FG/BRef/Savant (they all have 2015-2024/2025)
all_pf <- fg_j %>%
  full_join(bref_j, by = c("season", "team")) %>%
  full_join(savant_j, by = c("season", "team")) %>%
  left_join(our_pf %>% select(season, team, our_pf), by = c("season", "team"))

cat("\n=== Merged dataset ===\n")
cat("Rows:", nrow(all_pf), "\n")
cat("Complete cases (all 4 sources):", sum(complete.cases(all_pf %>% select(fg_overall, bref_pf, savant_runs, our_pf))), "\n")
cat("Seasons:", paste(sort(unique(all_pf$season)), collapse = ", "), "\n")

# ─── 4. Get runs scored per team-season (from MLB Stats API) ─────────────────

# Fetch from MLB Stats API
cat("\n=== Fetching runs scored from MLB Stats API ===\n")
library(jsonlite)

# First get team ID → abbreviation mapping
teams_meta <- fromJSON("https://statsapi.mlb.com/api/v1/teams?sportId=1&season=2024", flatten = FALSE)
team_id_map <- data.frame(
  team_id = teams_meta$teams$id,
  team_abbrev = teams_meta$teams$abbreviation,
  stringsAsFactors = FALSE
)

runs_list <- list()
for (yr in c(2015:2019, 2021:2025)) {
  url <- sprintf("https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=%d&standingsTypes=regularSeason", yr)
  tryCatch({
    resp <- fromJSON(url, flatten = FALSE)
    for (i in seq_len(nrow(resp$records))) {
      tr <- resp$records$teamRecords[[i]]
      if (is.data.frame(tr) && nrow(tr) > 0) {
        runs_list[[length(runs_list) + 1]] <- data.frame(
          season = yr,
          team_id = tr$team$id,
          team_name = tr$team$name,
          runs_scored = as.numeric(tr$runsScored),
          runs_allowed = as.numeric(tr$runsAllowed),
          games = as.numeric(tr$gamesPlayed),
          stringsAsFactors = FALSE
        )
      }
    }
  }, error = function(e) cat("  Error fetching", yr, ":", e$message, "\n"))
}

runs_df <- bind_rows(runs_list) %>%
  left_join(team_id_map, by = "team_id") %>%
  mutate(
    runs_per_game = runs_scored / games,
    total_runs_per_game_standings = (runs_scored + runs_allowed) / games
  )

cat("Runs data rows:", nrow(runs_df), "\n")
cat("Runs seasons:", paste(sort(unique(runs_df$season)), collapse = ", "), "\n")

# Normalize team abbreviations to match our standard
abbrev_fix <- c(
  "KC" = "KCR", "SF" = "SFG", "SD" = "SDP", "TB" = "TBR",
  "WSH" = "WSN", "OAK" = "ATH", "CWS" = "CWS", "AZ" = "ARI"
)

runs_df <- runs_df %>%
  mutate(team = ifelse(team_abbrev %in% names(abbrev_fix),
                       abbrev_fix[team_abbrev], team_abbrev))

# Merge runs with park factors
all_pf <- all_pf %>%
  left_join(runs_df %>% select(season, team, runs_scored, runs_allowed, games, runs_per_game),
            by = c("season", "team"))

cat("After adding runs - rows with runs data:", sum(!is.na(all_pf$runs_scored)), "\n")

# ─── 4b. Compute rolling averages for Savant and FG ──────────────────────────

cat("\n=== Computing rolling averages ===\n")

# Helper: compute trailing rolling average (including current year) for a given column
# Handles the 2020 gap by working on actual available seasons
compute_rolling_avg <- function(df, team_col, season_col, value_col, window, new_col) {
  df <- df %>% arrange(.data[[team_col]], .data[[season_col]])

  result <- df %>%
    group_by(.data[[team_col]]) %>%
    mutate(
      !!new_col := sapply(seq_len(n()), function(i) {
        current_season <- .data[[season_col]][i]
        # Get all prior seasons within window (including current)
        valid_idx <- which(
          .data[[season_col]] >= (current_season - window + 1) &
          .data[[season_col]] <= current_season &
          !is.na(.data[[value_col]])
        )
        if (length(valid_idx) >= 1) {
          mean(.data[[value_col]][valid_idx], na.rm = TRUE)
        } else {
          NA_real_
        }
      })
    ) %>%
    ungroup()

  result
}

# Savant rolling averages (from single-year savant_runs)
all_pf <- compute_rolling_avg(all_pf, "team", "season", "savant_runs", 3, "savant_3yr")
all_pf <- compute_rolling_avg(all_pf, "team", "season", "savant_runs", 5, "savant_5yr")

# FG rolling averages (from single-year fg_1yr)
all_pf <- compute_rolling_avg(all_pf, "team", "season", "fg_1yr", 3, "fg_3yr")
# fg_overall is already FG's published 5yr — we also compute our own 5yr from 1yr for comparison
all_pf <- compute_rolling_avg(all_pf, "team", "season", "fg_1yr", 5, "fg_5yr_calc")

# BRef rolling averages
all_pf <- compute_rolling_avg(all_pf, "team", "season", "bref_pf", 3, "bref_3yr")
all_pf <- compute_rolling_avg(all_pf, "team", "season", "bref_pf", 5, "bref_5yr")

cat("Rolling averages computed.\n")
cat("Savant 3yr non-NA:", sum(!is.na(all_pf$savant_3yr)), "\n")
cat("Savant 5yr non-NA:", sum(!is.na(all_pf$savant_5yr)), "\n")
cat("FG 3yr non-NA:", sum(!is.na(all_pf$fg_3yr)), "\n")
cat("BRef 3yr non-NA:", sum(!is.na(all_pf$bref_3yr)), "\n")

# ─── 5. ANALYSIS 1: Correlation with runs scored ─────────────────────────────

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║  ANALYSIS 1: Correlation with Total Runs (R + RA per game)     ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n\n")

# Total runs at a venue = runs_scored + runs_allowed (both home and away contribute)
all_pf <- all_pf %>%
  mutate(total_runs_per_game = (runs_scored + runs_allowed) / games)

# Common years for each source
# Our PFs: 2018-2025; Savant: 2015-2024; FG: 2015-2025; BRef: 2015-2025

sources <- list(
  "Savant 1yr"   = list(col = "savant_runs"),
  "Savant 3yr"   = list(col = "savant_3yr"),
  "Savant 5yr"   = list(col = "savant_5yr"),
  "FG 1yr"       = list(col = "fg_1yr"),
  "FG 3yr"       = list(col = "fg_3yr"),
  "FG 5yr (pub)" = list(col = "fg_overall"),
  "BRef 1yr"     = list(col = "bref_pf"),
  "BRef 3yr"     = list(col = "bref_3yr"),
  "BRef 5yr"     = list(col = "bref_5yr"),
  "Ours (Model)" = list(col = "our_pf")
)

cat("Correlation of park factor with total R/G at venue (Pearson / Spearman):\n")
cat(sprintf("%-20s  %6s  %7s  %7s  %s\n", "Source", "N", "Pearson", "Spearman", "Years"))
cat(paste(rep("─", 70), collapse = ""), "\n")

runs_corr_results <- list()
for (nm in names(sources)) {
  col <- sources[[nm]]$col
  tmp <- all_pf %>% filter(!is.na(.data[[col]]), !is.na(total_runs_per_game))
  if (nrow(tmp) > 5) {
    pr <- cor(tmp[[col]], tmp$total_runs_per_game, method = "pearson")
    sr <- cor(tmp[[col]], tmp$total_runs_per_game, method = "spearman")
    yrs <- paste(range(tmp$season), collapse = "-")
    cat(sprintf("%-20s  %6d  %7.3f  %7.3f  %s\n", nm, nrow(tmp), pr, sr, yrs))
    runs_corr_results[[nm]] <- data.frame(source = nm, n = nrow(tmp),
                                           pearson = pr, spearman = sr, years = yrs)
  }
}

# ─── 6. ANALYSIS 2: Year-over-Year Stability ─────────────────────────────────

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║  ANALYSIS 2: Year-over-Year Stability (Adjacent-Season Corr)   ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n\n")

# For each source, correlate year_t PF with year_t+1 PF for same team
compute_yoy_stability <- function(df, pf_col, source_name) {
  wide <- df %>%
    filter(!is.na(.data[[pf_col]])) %>%
    select(team, season, pf = all_of(pf_col)) %>%
    arrange(team, season)

  # Create adjacent-year pairs
  pairs <- wide %>%
    inner_join(wide, by = "team", suffix = c("_t", "_t1")) %>%
    filter(season_t1 == season_t + 1 |
           (season_t == 2019 & season_t1 == 2021)) %>%  # skip 2020
    filter(season_t >= 2015, season_t1 <= 2025)

  if (nrow(pairs) < 5) return(NULL)

  pr <- cor(pairs$pf_t, pairs$pf_t1, method = "pearson")
  sr <- cor(pairs$pf_t, pairs$pf_t1, method = "spearman")

  # Also compute per-pair-year correlations
  pair_years <- pairs %>%
    group_by(season_t) %>%
    summarize(
      n = n(),
      pearson = cor(pf_t, pf_t1, method = "pearson"),
      spearman = cor(pf_t, pf_t1, method = "spearman"),
      .groups = "drop"
    )

  list(
    overall = data.frame(source = source_name, n_pairs = nrow(pairs),
                         n_transitions = length(unique(pairs$season_t)),
                         pearson = pr, spearman = sr),
    by_year = pair_years %>% mutate(source = source_name)
  )
}

yoy_sources <- list(
  "Savant 1yr"   = "savant_runs",
  "Savant 3yr"   = "savant_3yr",
  "Savant 5yr"   = "savant_5yr",
  "FG 1yr"       = "fg_1yr",
  "FG 3yr"       = "fg_3yr",
  "FG 5yr (pub)" = "fg_overall",
  "BRef 1yr"     = "bref_pf",
  "BRef 3yr"     = "bref_3yr",
  "BRef 5yr"     = "bref_5yr",
  "Ours (Model)" = "our_pf"
)

cat("Overall adjacent-season stability (year_t vs year_t+1, same team):\n")
cat(sprintf("%-20s  %6s  %12s  %7s  %7s\n", "Source", "Pairs", "Transitions", "Pearson", "Spearman"))
cat(paste(rep("─", 70), collapse = ""), "\n")

yoy_results <- list()
yoy_by_year <- list()
for (nm in names(yoy_sources)) {
  res <- compute_yoy_stability(all_pf, yoy_sources[[nm]], nm)
  if (!is.null(res)) {
    cat(sprintf("%-20s  %6d  %12d  %7.3f  %7.3f\n",
                nm, res$overall$n_pairs, res$overall$n_transitions,
                res$overall$pearson, res$overall$spearman))
    yoy_results[[nm]] <- res$overall
    yoy_by_year[[nm]] <- res$by_year
  }
}

# Per-transition-year detail: show focused subset (1yr vs 3yr for each source + ours)
cat("\n\nPer-transition-year Pearson correlations (1yr vs 3yr for each source):\n")
yoy_detail <- bind_rows(yoy_by_year)
detail_sources <- c("Savant 1yr", "Savant 3yr", "FG 1yr", "FG 3yr", "BRef 1yr", "BRef 3yr", "Ours (Model)")
yoy_wide <- yoy_detail %>%
  filter(source %in% detail_sources) %>%
  select(source, season_t, pearson) %>%
  pivot_wider(names_from = source, values_from = pearson)

cat(sprintf("%-12s", "Year→Year+1"))
for (nm in detail_sources) {
  cat(sprintf("  %12s", nm))
}
cat("\n")
cat(paste(rep("─", 12 + 14 * length(detail_sources)), collapse = ""), "\n")

for (i in seq_len(nrow(yoy_wide))) {
  yr <- yoy_wide$season_t[i]
  yr_next <- ifelse(yr == 2019, 2021, yr + 1)
  cat(sprintf("%-12s", paste0(yr, "→", yr_next)))
  for (nm in detail_sources) {
    val <- if (nm %in% names(yoy_wide)) yoy_wide[[nm]][i] else NA
    if (is.na(val)) {
      cat(sprintf("  %12s", "—"))
    } else {
      cat(sprintf("  %12.3f", val))
    }
  }
  cat("\n")
}

# ─── 7. ANALYSIS 3: Mean Absolute Year-over-Year Change ──────────────────────

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║  ANALYSIS 3: Mean Absolute YoY Change (lower = more stable)    ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n\n")

cat(sprintf("%-20s  %10s  %10s  %10s\n", "Source", "Mean |Δ|", "Median |Δ|", "SD of Δ"))
cat(paste(rep("─", 60), collapse = ""), "\n")

for (nm in names(yoy_sources)) {
  col <- yoy_sources[[nm]]
  wide <- all_pf %>%
    filter(!is.na(.data[[col]])) %>%
    select(team, season, pf = all_of(col)) %>%
    arrange(team, season)

  pairs <- wide %>%
    inner_join(wide, by = "team", suffix = c("_t", "_t1")) %>%
    filter(season_t1 == season_t + 1 | (season_t == 2019 & season_t1 == 2021)) %>%
    filter(season_t >= 2015, season_t1 <= 2025) %>%
    mutate(delta = pf_t1 - pf_t)

  if (nrow(pairs) > 0) {
    cat(sprintf("%-20s  %10.2f  %10.2f  %10.2f\n",
                nm, mean(abs(pairs$delta)), median(abs(pairs$delta)), sd(pairs$delta)))
  }
}

# ─── 8. Save results ─────────────────────────────────────────────────────────

cat("\n=== Done! ===\n")
