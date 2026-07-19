#!/usr/bin/env Rscript
# derive_streamonator_weights_2025.R
#
# Same logic as derive_streamonator_weights.R but for the full 2025 season.
# Key differences vs 2026 version:
#   - Season: 2025 (full season, Mar 20 – Sep 28 2025)
#   - Park factors: era-correct 2025 via team_park_era_audit.csv join
#   - FG URLs: season=2025
#   - Output filenames: *_2025.*

suppressPackageStartupMessages({
  library(jsonlite)
})

source(file.path("R", "utils.R"))
source(file.path("R", "sp_skillz.R"))

# ── Paths ──────────────────────────────────────────────────────────────────────
CACHE_DIR   <- file.path("data", "processed", "streamonator_weight_analysis")
PF_ERA_PATH <- file.path("data", "processed", "park_factors",
                         "park_factors_savant_style_with_id.csv")
ERA_MAP_PATH <- file.path("data", "processed", "park_factors",
                          "team_park_era_audit.csv")
OUT_STARTS  <- file.path(CACHE_DIR, "starts_2025.csv")
OUT_GRID    <- file.path(CACHE_DIR, "weight_grid_2025.csv")

dir.create(CACHE_DIR, showWarnings = FALSE, recursive = TRUE)

SEASON      <- 2025
SEASON_START <- "2025-03-18"
SEASON_END   <- "2025-09-28"

# ── Cache helpers ──────────────────────────────────────────────────────────────
read_cache  <- function(p) if (file.exists(p)) tryCatch(readRDS(p), error = function(e) NULL) else NULL
write_cache <- function(obj, p) tryCatch(saveRDS(obj, p), error = function(e) NULL)

# ── Team ID → abbreviation ─────────────────────────────────────────────────────
MLB_ID_TO_ABR <- c(
  `108`="LAA", `109`="AZ",  `110`="BAL", `111`="BOS", `112`="CHC",
  `113`="CIN", `114`="CLE", `115`="COL", `116`="DET", `117`="HOU",
  `118`="KCR", `119`="LAD", `120`="WSH", `121`="NYM", `133`="ATH",
  `134`="PIT", `135`="SDP", `136`="SEA", `137`="SFG", `138`="STL",
  `139`="TBR", `140`="TEX", `141`="TOR", `142`="MIN", `143`="PHI",
  `144`="ATL", `145`="CHW", `146`="MIA", `147`="NYY", `158`="MIL"
)
mlb_abr <- function(id) unname(MLB_ID_TO_ABR[as.character(id)])

TEAM_MAP <- c(
  ARI="AZ",  ARZ="AZ",  AZ="AZ",
  ATH="ATH", OAK="ATH",
  BAL="BAL", BOS="BOS",
  CHC="CHC", CHN="CHC",
  CHW="CHW", CHA="CHW", CWS="CHW",
  CIN="CIN", CLE="CLE", CLV="CLE",
  COL="COL", DET="DET", HOU="HOU",
  KC="KCR",  KCR="KCR",
  LAA="LAA", LAD="LAD", LA="LAD",
  MIA="MIA", FLA="MIA",
  MIL="MIL", MIN="MIN",
  NYM="NYM", NYY="NYY", PHI="PHI", PIT="PIT",
  SD="SDP",  SDP="SDP",
  SEA="SEA",
  SF="SFG",  SFG="SFG",
  STL="STL",
  TB="TBR",  TBR="TBR", TAM="TBR",
  TEX="TEX", TOR="TOR",
  WSH="WSH", WSN="WSH", WAS="WSH"
)
norm_team <- function(x) {
  out <- TEAM_MAP[toupper(trimws(as.character(x)))]
  out[is.na(out)] <- toupper(trimws(as.character(x)))[is.na(out)]
  unname(out)
}

norm_name <- function(x) {
  x <- iconv(as.character(x), to = "ASCII//TRANSLIT", sub = "")
  gsub("[^a-z ]", "", tolower(trimws(x)))
}
last_name <- function(x) {
  vapply(strsplit(x, " "), function(p) if (length(p)) p[length(p)] else "", character(1))
}

# ── 1. Fetch 2025 schedule → completed game PKs ────────────────────────────────
message("── Step 1: Fetching 2025 schedule ──────────────────────────────────────")
sched_cache <- file.path(CACHE_DIR, "schedule_2025.rds")
sched_raw   <- read_cache(sched_cache)

if (is.null(sched_raw)) {
  sched_url <- paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?sportId=1&gameType=R",
    "&startDate=", SEASON_START,
    "&endDate=",   SEASON_END
  )
  message("  Fetching: ", sched_url)
  sched_raw <- tryCatch(
    fromJSON(sched_url, simplifyVector = FALSE),
    error = function(e) stop("Schedule fetch failed: ", conditionMessage(e))
  )
  write_cache(sched_raw, sched_cache)
  message("  Cached schedule.")
} else {
  message("  Loaded schedule from cache.")
}

game_index <- do.call(rbind, lapply(sched_raw$dates, function(day) {
  do.call(rbind, lapply(day$games, function(g) {
    state <- tryCatch(g$status$abstractGameState, error = function(e) "")
    if (!identical(state, "Final")) return(NULL)
    data.frame(
      game_pk   = as.integer(g$gamePk),
      game_date = as.Date(day$date),
      home_id   = as.integer(tryCatch(g$teams$home$team$id, error = function(e) NA)),
      away_id   = as.integer(tryCatch(g$teams$away$team$id, error = function(e) NA)),
      stringsAsFactors = FALSE
    )
  }))
}))
game_index <- game_index[!is.na(game_index$home_id) & !is.na(game_index$away_id), ]
message(sprintf("  Found %d completed games.", nrow(game_index)))

# ── 2. Fetch boxscores + extract starter lines ─────────────────────────────────
message("── Step 2: Fetching boxscores ──────────────────────────────────────────")

parse_starter_side <- function(side, ha, meta) {
  pitchers <- side$pitchers
  if (!length(pitchers)) return(NULL)

  sp_id  <- as.character(pitchers[[1]])
  player <- side$players[[paste0("ID", sp_id)]]
  if (is.null(player)) return(NULL)

  ps <- tryCatch(player$stats$pitching, error = function(e) NULL)
  if (is.null(ps)) return(NULL)

  ip_raw <- suppressWarnings(as.numeric(ps$inningsPitched %||% NA))
  ip_dec <- baseball_ip_to_decimal(ip_raw)

  data.frame(
    game_pk       = meta$game_pk,
    game_date     = meta$game_date,
    pitcher_id    = as.integer(sp_id),
    pitcher_name  = trimws(player$person$fullName %||% NA_character_),
    pitcher_team  = mlb_abr(if (ha == "home") meta$home_id else meta$away_id),
    opponent_team = mlb_abr(if (ha == "home") meta$away_id else meta$home_id),
    home_away     = if (ha == "home") "H" else "A",
    ip            = ip_dec,
    er            = suppressWarnings(as.integer(ps$earnedRuns  %||% NA)),
    h             = suppressWarnings(as.integer(ps$hits        %||% NA)),
    bb            = suppressWarnings(as.integer(ps$baseOnBalls %||% NA)),
    k             = suppressWarnings(as.integer(ps$strikeOuts  %||% NA)),
    win           = suppressWarnings(as.integer(ps$wins        %||% 0L)) >= 1L,
    stringsAsFactors = FALSE
  )
}

starter_rows <- list()
for (i in seq_len(nrow(game_index))) {
  meta    <- game_index[i, ]
  pk      <- meta$game_pk
  cache_f <- file.path(CACHE_DIR, sprintf("box2025_%d.rds", pk))

  box <- read_cache(cache_f)
  if (is.null(box)) {
    url <- sprintf("https://statsapi.mlb.com/api/v1/game/%d/boxscore", pk)
    box <- tryCatch(fromJSON(url, simplifyVector = FALSE), error = function(e) NULL)
    if (!is.null(box)) write_cache(box, cache_f)
    Sys.sleep(0.05)
  }

  if (is.null(box) || is.null(box$teams)) next

  for (ha in c("home", "away")) {
    row <- tryCatch(
      parse_starter_side(box$teams[[ha]], ha, meta),
      error = function(e) NULL
    )
    if (!is.null(row) && nzchar(row$pitcher_name %||% "")) {
      starter_rows[[length(starter_rows) + 1L]] <- row
    }
  }

  if (i %% 50 == 0 || i == nrow(game_index)) {
    message(sprintf("  %d / %d games processed (%d starts so far)",
                    i, nrow(game_index), length(starter_rows)))
  }
}

starts <- do.call(rbind, starter_rows)
rownames(starts) <- NULL
message(sprintf("  Extracted %d starter lines total.", nrow(starts)))

# ── 3. Good start score (0–5) ─────────────────────────────────────────────────
message("── Step 3: Computing good start scores ─────────────────────────────────")

starts$pitcher_team  <- norm_team(starts$pitcher_team)
starts$opponent_team <- norm_team(starts$opponent_team)

starts$whip <- ifelse(
  !is.na(starts$ip) & starts$ip > 0,
  (starts$h + starts$bb) / starts$ip,
  Inf
)

starts$ip_ok   <- !is.na(starts$ip)   & starts$ip >= 5
starts$k_ok    <- !is.na(starts$k)    & !is.na(starts$ip) &
                  starts$k >= (floor(starts$ip) - 1)
starts$er_ok   <- !is.na(starts$er)   & starts$er   <= 3
starts$whip_ok <- !is.na(starts$whip) & starts$whip <= 1.20
starts$w_ok    <- !is.na(starts$win)  & starts$win

starts$good_start_score <-
  as.integer(starts$ip_ok)   +
  as.integer(starts$k_ok)    +
  as.integer(starts$er_ok)   +
  as.integer(starts$whip_ok) +
  as.integer(starts$w_ok)

message("  Score distribution (0=worst, 5=best):")
print(table(starts$good_start_score))

# ── 4. Load + index SP Skillz ─────────────────────────────────────────────────
message("── Step 4: Fetching 2025 SP Skillz ─────────────────────────────────────")
spz_cache <- file.path(CACHE_DIR, "spz_2025_scores.rds")
spz_scores <- read_cache(spz_cache)

if (is.null(spz_scores)) {
  spz_url <- paste0(
    "https://www.fangraphs.com/leaders/major-league",
    "?pos=all&stats=pit&lg=all",
    "&type=c%2C7%2C8%2C14%2C13%2C55%2C57%2C-1%2C6%2C62%2C122%2C42%2C-1",
    "%2C120%2C331%2C121%2C29%2C31%2C-1%2C105%2C110%2C-1%2C386%2C387%2C388",
    "&month=0&ind=0&team=0&rost=0&age=0&filter&players=0",
    "&startdate&enddate&v_cr=legacy",
    "&qual=0&season1=2025&season=2025"
  )
  message("  Fetching from FanGraphs...")
  result <- tryCatch(
    fetch_and_score_sp_skillz(leaderboard_url = spz_url),
    error = function(e) stop("SP Skillz fetch failed: ", conditionMessage(e))
  )
  spz_scores <- result$scores
  write_cache(spz_scores, spz_cache)
  message(sprintf("  Fetched and scored %d pitchers. Cached.", nrow(spz_scores)))
} else {
  message(sprintf("  Loaded SP Skillz from cache (%d pitchers).", nrow(spz_scores)))
}

s     <- suppressWarnings(as.numeric(spz_scores$sp_skillz_score_stabilized))
mu    <- mean(s, na.rm = TRUE)
sigma <- sd(s, na.rm = TRUE)
spz_scores$sp_skillz_index <- if (is.na(sigma) || sigma == 0) {
  ifelse(is.na(s), NA_real_, 100)
} else {
  round(100 + (s - mu) / sigma * 10, 1)
}
spz_scores$name_key <- norm_name(spz_scores$player_name)

starts$name_key <- norm_name(starts$pitcher_name)
spz_idx <- match(starts$name_key, spz_scores$name_key)

unmatched <- which(is.na(spz_idx))
if (length(unmatched)) {
  s_last  <- last_name(starts$name_key[unmatched])
  sz_last <- last_name(spz_scores$name_key)
  for (j in seq_along(unmatched)) {
    hits <- which(sz_last == s_last[j])
    if (length(hits) == 1L) spz_idx[unmatched[j]] <- hits[1L]
  }
}

starts$sp_skillz_index <- spz_scores$sp_skillz_index[spz_idx]
starts$spz_placeholder  <- is.na(starts$sp_skillz_index)
starts$sp_skillz_index[starts$spz_placeholder] <- 100

message(sprintf("  SP Skillz matched: %d / %d starts (%.0f%%; %d defaulted to 100)",
                sum(!starts$spz_placeholder), nrow(starts),
                100 * mean(!starts$spz_placeholder),
                sum(starts$spz_placeholder)))

# ── 5. Load + join Park Factor (era-correct for 2025) ─────────────────────────
message("── Step 5: Joining Park Factors (2025 era-correct) ─────────────────────")
pf_era  <- read.csv(PF_ERA_PATH,  stringsAsFactors = FALSE)
era_map <- read.csv(ERA_MAP_PATH, stringsAsFactors = FALSE)

era_2025 <- era_map[era_map$season == 2025, c("home_team", "park_era_id")]
pf_2025  <- merge(era_2025, pf_era[, c("park_era_id", "overall_pf_idx_100", "team")],
                  by = "park_era_id")
pf_2025$team_norm <- norm_team(pf_2025$home_team)

park_team <- ifelse(starts$home_away == "H", starts$pitcher_team, starts$opponent_team)
pf_match  <- match(park_team, pf_2025$team_norm)

starts$park_factor_raw <- suppressWarnings(
  as.numeric(pf_2025$overall_pf_idx_100[pf_match])
)
starts$park_factor_inv <- ifelse(
  is.na(starts$park_factor_raw), NA_real_,
  200 - starts$park_factor_raw
)

message(sprintf("  Park Factor matched: %d / %d starts",
                sum(!is.na(starts$park_factor_raw)), nrow(starts)))

# ── 6. Fetch + score Team Rater ────────────────────────────────────────────────
message("── Step 6: Fetching 2025 Team Rater ────────────────────────────────────")
tr_cache <- file.path(CACHE_DIR, "team_rater_2025.rds")
tr_df    <- read_cache(tr_cache)

if (is.null(tr_df)) {
  tr_url <- paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&stats=bat&lg=all&qual=0&ind=0",
    "&team=0,ts&rost=0&players=0",
    "&type=c,6,11,12,34,35,50",
    "&season=2025&season1=2025&month=0",
    "&pageitems=50&pagenum=1"
  )
  message("  Fetching from FanGraphs...")

  tr_raw <- tryCatch(fromJSON(tr_url, simplifyVector = TRUE), error = function(e) NULL)

  if (is.null(tr_raw)) {
    tmp <- tempfile(fileext = ".json")
    on.exit(unlink(tmp), add = TRUE)
    system2("curl", c(
      "-sS", "-L", "--fail", "--compressed", "--max-time", "30",
      "-A", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
      "-H", "Accept: application/json, text/plain, */*",
      "-H", "Referer: https://www.fangraphs.com/leaders/major-league",
      tr_url, "-o", tmp
    ))
    if (file.exists(tmp) && file.size(tmp) > 100)
      tr_raw <- tryCatch(fromJSON(tmp, simplifyVector = TRUE), error = function(e) NULL)
  }

  if (is.null(tr_raw)) stop("Team Rater fetch failed.")

  df_raw <- if (is.data.frame(tr_raw)) tr_raw else
            if (is.data.frame(tr_raw$data)) tr_raw$data else
            stop("Unexpected Team Rater API response structure.")

  cn <- tolower(names(df_raw))
  col_num <- function(...) {
    idx <- match(tolower(c(...)), cn)
    idx <- idx[!is.na(idx)]
    if (!length(idx)) rep(NA_real_, nrow(df_raw))
    else suppressWarnings(as.numeric(df_raw[[idx[1L]]]))
  }
  col_chr <- function(...) {
    idx <- match(tolower(c(...)), cn)
    idx <- idx[!is.na(idx)]
    if (!length(idx)) rep(NA_character_, nrow(df_raw))
    else trimws(as.character(df_raw[[idx[1L]]]))
  }

  tr_df <- data.frame(
    abbr   = col_chr("teamnameabb", "teamname", "team", "teamabbr"),
    HR     = col_num("hr"),
    R      = col_num("r"),
    BB_pct = col_num("bb%"),
    K_pct  = col_num("k%"),
    wOBA   = col_num("woba"),
    stringsAsFactors = FALSE
  )
  tr_df <- tr_df[!is.na(tr_df$abbr) & nzchar(tr_df$abbr), , drop = FALSE]

  DIRS <- c(HR = 1, R = 1, BB_pct = 1, K_pct = -1, wOBA = 1)
  z_mat <- do.call(cbind, lapply(names(DIRS), function(col) {
    x  <- tr_df[[col]]
    mu <- mean(x, na.rm = TRUE); sg <- sd(x, na.rm = TRUE)
    if (is.na(sg) || sg == 0) return(rep(0, nrow(tr_df)))
    DIRS[[col]] * (x - mu) / sg
  }))
  z_sum <- rowSums(z_mat, na.rm = TRUE)
  z_mu  <- mean(z_sum, na.rm = TRUE)
  z_sg  <- sd(z_sum, na.rm = TRUE)

  tr_df$team_rater_index <- if (is.na(z_sg) || z_sg == 0) {
    rep(100, nrow(tr_df))
  } else {
    round(100 + (z_sum - z_mu) / z_sg * 10, 1)
  }
  tr_df$team_norm <- norm_team(tr_df$abbr)

  write_cache(tr_df, tr_cache)
  message(sprintf("  Fetched %d teams. Cached.", nrow(tr_df)))
} else {
  message(sprintf("  Loaded Team Rater from cache (%d teams).", nrow(tr_df)))
}

tr_match <- match(starts$opponent_team, tr_df$team_norm)
starts$team_rater_raw <- tr_df$team_rater_index[tr_match]
starts$team_rater_inv <- ifelse(
  is.na(starts$team_rater_raw), NA_real_,
  200 - starts$team_rater_raw
)
starts$tr_placeholder <- is.na(starts$team_rater_raw)
starts$team_rater_inv[starts$tr_placeholder] <- 100

message(sprintf("  Team Rater matched: %d / %d starts (%d defaulted to 100)",
                sum(!starts$tr_placeholder), nrow(starts),
                sum(starts$tr_placeholder)))

# ── Save analysis dataset ──────────────────────────────────────────────────────
write.csv(starts, OUT_STARTS, row.names = FALSE)
message(sprintf("\n  Analysis dataset saved: %s (%d rows)", OUT_STARTS, nrow(starts)))

# ── 7. Grid search ────────────────────────────────────────────────────────────
message("── Step 7: Grid search over (w_sp, w_tr, w_pf) ────────────────────────")

grid_raw <- expand.grid(
  w_sp = seq(0, 1, by = 0.1),
  w_tr = seq(0, 1, by = 0.1),
  w_pf = seq(0, 1, by = 0.1)
)
grid <- grid_raw[abs(rowSums(grid_raw) - 1) < 1e-9, , drop = FALSE]
rownames(grid) <- NULL
message(sprintf("  Evaluating %d weight combinations...", nrow(grid)))

outcome <- starts$good_start_score

grid_results <- do.call(rbind, lapply(seq_len(nrow(grid)), function(j) {
  w_sp <- grid$w_sp[j]; w_tr <- grid$w_tr[j]; w_pf <- grid$w_pf[j]

  scores <- vapply(seq_len(nrow(starts)), function(i) {
    vals <- c(starts$sp_skillz_index[i],
              starts$team_rater_inv[i],
              starts$park_factor_inv[i])
    wts  <- c(w_sp, w_tr, w_pf)
    ok   <- !is.na(vals) & wts > 0
    if (!any(ok)) return(NA_real_)
    sum(vals[ok] * wts[ok]) / sum(wts[ok])
  }, numeric(1))

  valid <- !is.na(scores) & !is.na(outcome)
  if (sum(valid) < 10L) return(NULL)

  spearman <- suppressWarnings(
    cor(scores[valid], outcome[valid], method = "spearman")
  )

  top_cut  <- quantile(scores[valid], 0.75, na.rm = TRUE)
  mean_top <- mean(outcome[valid & scores >= top_cut], na.rm = TRUE)

  data.frame(
    w_sp       = w_sp,
    w_tr       = w_tr,
    w_pf       = w_pf,
    spearman   = round(spearman, 4),
    mean_top25 = round(mean_top, 3),
    n          = sum(valid),
    stringsAsFactors = FALSE
  )
}))

grid_results <- grid_results[order(-grid_results$spearman), , drop = FALSE]
rownames(grid_results) <- NULL
write.csv(grid_results, OUT_GRID, row.names = FALSE)

# ── 8. Report ──────────────────────────────────────────────────────────────────
message("\n══════════════════════════════════════════════════════════════════")
message("  STREAMONATOR WEIGHT OPTIMIZATION — 2025 RESULTS")
message("══════════════════════════════════════════════════════════════════")
message(sprintf("  Starts analyzed:  %d  (%s – %s)",
                nrow(starts), SEASON_START, SEASON_END))
message(sprintf("  With SP Skillz:   %d (%.0f%%)",
                sum(!starts$spz_placeholder), 100 * mean(!starts$spz_placeholder)))
message(sprintf("  With Team Rater:  %d (%.0f%%)",
                sum(!starts$tr_placeholder), 100 * mean(!starts$tr_placeholder)))
message(sprintf("  With Park Factor: %d (%.0f%%)",
                sum(!is.na(starts$park_factor_raw)),
                100 * mean(!is.na(starts$park_factor_raw))))
message("")
message("  Good start score distribution:")
dist_tbl <- table(starts$good_start_score)
for (sc in names(dist_tbl)) {
  pct <- round(100 * dist_tbl[[sc]] / nrow(starts), 1)
  message(sprintf("    Score %s: %d starts (%s%%)", sc, dist_tbl[[sc]], pct))
}
message("")
message("  Top 15 weight combos by Spearman correlation:")
message("  (w_sp + w_tr + w_pf = 1; current default is 0.33 : 0.33 : 0.33)")
print(head(grid_results, 15), row.names = FALSE)

baseline_row <- grid_results[
  abs(grid_results$w_sp - 1/3) < 0.06 &
  abs(grid_results$w_tr - 1/3) < 0.06 &
  abs(grid_results$w_pf - 1/3) < 0.06, ]
if (nrow(baseline_row) > 0) {
  message("\n  Current baseline (equal weights):")
  print(baseline_row[1, ], row.names = FALSE)
  best_row <- grid_results[1, ]
  delta    <- round(best_row$spearman - baseline_row$spearman[1], 4)
  message(sprintf("  Best vs baseline: Δspearman = %+.4f", delta))
}

message("")
message(sprintf("  Full grid saved: %s", OUT_GRID))
message(sprintf("  Starts data:     %s", OUT_STARTS))
message("══════════════════════════════════════════════════════════════════")
