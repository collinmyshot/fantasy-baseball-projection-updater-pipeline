#!/usr/bin/env Rscript
# build_hitter_map.R
# Merge bat-tracking (swing path), swing-timing (miss geometry), FanGraphs outcomes,
# and Savant batted-ball (pull-air) into ONE tidy player-year table, then build an
# input -> outcome correlation map and run the "early + under = pull-air" test.
#
# Grain: one row per (MLBAM id, year). Swing metrics carry _all / _bip / _whiff
# suffixes for the three contact-state cuts.
#
# USAGE (interactive):
#   source("scripts/article_research/build_hitter_map.R")
#   tbl <- build_hitter_table()                       # merged table (also written to data/processed)
#   cm  <- correlation_map(tbl)                        # input x outcome matrix + heatmap PNG
#   pa  <- pull_air_test(tbl)                          # early+under headline test
#
# USAGE (CLI):
#   Rscript scripts/article_research/build_hitter_map.R                 # builds table + map + test, writes outputs

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(ggplot2)
})

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR <- "/Users/ckaufman/Downloads"
OUT_DIR  <- "/Users/ckaufman/Documents/New project/data/processed/swing_map"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

MIN_PA     <- 100   # FanGraphs PA floor for eligibility
MIN_SWINGS <- 300   # min competitive swings (n_swings_all) — standard floor (reduces geometry noise)

# ── Low-level readers ────────────────────────────────────────────────────────
.read_clean <- function(file) {
  d <- suppressWarnings(read_csv(file.path(DATA_DIR, file),
                                 show_col_types = FALSE, name_repair = "minimal"))
  names(d)[1] <- "id"   # strip BOM-mangled first column name
  d
}

# select id+year + a set of columns, renaming them with a cut suffix
.pick_cut <- function(df, cols, suffix) {
  cols <- intersect(cols, names(df))
  df |>
    select(id, year, all_of(cols)) |>
    rename_with(~ paste0(.x, suffix), all_of(cols))
}

# Columns shared by the swing-timing cuts (miss geometry decomposition)
TIMING_COLS <- c(
  "miss_distance",
  "perfect_percent", "flawed_percent",                               # article-validated combos
  "tied_up_percent", "centered_percent", "flailed_percent",          # in/out (X)
  "early_percent", "on_time_percent", "late_percent",                # timing (Y)
  "over_percent", "lined_up_percent", "under_percent",               # over/under (Z)
  "avg_x_tied_up", "avg_x_flail", "avg_y_early", "avg_y_late",        # miss magnitudes
  "avg_z_over", "avg_z_under",
  "n_swings"
)
# Bat-tracking metrics valid on contact cuts vs whiff cut
BT_COLS_FULL <- c("avg_bat_speed", "hard_swing_rate", "swing_length",
                  "squared_up_per_swing", "blast_per_swing")
BT_COLS_MECH <- c("avg_bat_speed", "hard_swing_rate", "swing_length")  # whiff cut: mechanics only


# ── Build the merged table ───────────────────────────────────────────────────
build_hitter_table <- function(min_pa = MIN_PA, min_swings = MIN_SWINGS, write = TRUE) {

  # Swing-timing (miss geometry) — three cuts
  st_all   <- .read_clean("bat-tracking-swing-timing_2024-26_all.csv")
  st_bip   <- .read_clean("bat-tracking-swing-timing_2024-26_bip.csv")
  st_whiff <- .read_clean("bat-tracking-swing-timing_2024-26_whiff.csv")

  base <- st_all |>
    transmute(id, year, name, team_name,
              bat_side = bat_side_formatted,
              whiff_rate, competitive_percent)

  tbl <- base |>
    left_join(.pick_cut(st_all,   TIMING_COLS, "_all"),   by = c("id", "year")) |>
    left_join(.pick_cut(st_bip,   TIMING_COLS, "_bip"),   by = c("id", "year")) |>
    left_join(.pick_cut(st_whiff, TIMING_COLS, "_whiff"), by = c("id", "year"))

  # Bat-tracking (swing path) — three cuts
  bt_all   <- .read_clean("bat-tracking_2024-26_all.csv")
  bt_bip   <- .read_clean("bat-tracking_2024-26_bip.csv")
  bt_whiff <- .read_clean("bat-tracking_2024-26_whiff.csv")

  tbl <- tbl |>
    # all cut also carries aggregate sample/quality fields
    left_join(
      bt_all |> select(id, year, all_of(BT_COLS_FULL),
                       swings_competitive, contact, swords, batter_run_value) |>
        rename_with(~ paste0(.x, "_all"), all_of(BT_COLS_FULL)),
      by = c("id", "year")
    ) |>
    left_join(.pick_cut(bt_bip,   BT_COLS_FULL, "_bip"),   by = c("id", "year")) |>
    left_join(.pick_cut(bt_whiff, BT_COLS_MECH, "_whiff"), by = c("id", "year"))

  # FanGraphs outcomes — join on MLBAMID + Season
  fg <- suppressWarnings(read_csv(file.path(DATA_DIR, "FG_lotsastats.csv"),
                                  show_col_types = FALSE, name_repair = "minimal"))
  names(fg)[1] <- "Season"
  fg_clean <- fg |>
    transmute(
      id = MLBAMID, year = Season, pa = PA, g = G, age = Age,
      # value
      xwoba = xwOBA, woba = wOBA, xba = xBA, xslg = xSLG, slg = SLG, obp = OBP,
      avg = AVG, babip = BABIP, iso = ISO,
      # quality of contact
      barrel_pct = `Barrel%`, hardhit_pct = `HardHit%`, ev90 = EV90, max_ev = maxEV, la = LA,
      # batted-ball mix (FG)
      gb_pct = `GB%`, fb_pct = `FB%`, iffb_pct = `IFFB%`, hr_fb = `HR/FB`,
      ld_pct = pmax(0, 1 - `GB%` - `FB%`),
      # spray (FG)
      pull_pct = `Pull%`, cent_pct = `Cent%`, oppo_pct = `Oppo%`,
      # discipline
      k_pct = `K%`, bb_pct = `BB%`,
      o_swing = `O-Swing% (sc)`, z_swing = `Z-Swing% (sc)`, swing_pct = `Swing% (sc)`,
      contact_pct = `Contact% (sc)`, z_contact = `Z-Contact% (sc)`, o_contact = `O-Contact% (sc)`,
      # fantasy counting
      hr = HR, r = R, rbi = RBI, sb = SB,
      # denominators (for per-PA constructions — Gallo problem)
      events = Events
    )
  tbl <- tbl |> left_join(fg_clean, by = c("id", "year"))

  # Savant batted-ball (pull-air + direction x launch crosstab) — join on id + year
  sv <- bind_rows(lapply(2024:2026, function(y)
    .read_clean(sprintf("savant_batted_ball_%d.csv", y))))
  sv_clean <- sv |>
    transmute(id, year,
              sv_bbe = bbe,
              pull_air_rate, pull_gb_rate, oppo_air_rate, straight_air_rate,
              sv_air = air_rate, sv_fb = fb_rate, sv_gb = gb_rate,
              sv_pull = pull_rate, sv_oppo = oppo_rate)
  tbl <- tbl |> left_join(sv_clean, by = c("id", "year"))

  # Eligibility filter (PA floor + optional competitive-swings floor)
  tbl <- tbl |> filter(!is.na(pa), pa >= min_pa)
  if (min_swings > 0) tbl <- tbl |> filter(!is.na(n_swings_all), n_swings_all >= min_swings)

  if (write) {
    out <- file.path(OUT_DIR, "hitter_swing_map.csv")
    write_csv(tbl, out)
    message(sprintf("Wrote %d rows x %d cols -> %s", nrow(tbl), ncol(tbl), out))
  }
  tbl
}


# ── Default input / outcome column sets (edit freely) ─────────────────────────
# INPUTS = mechanical swing properties (how the swing works)
DEFAULT_INPUTS <- c(
  # bat speed & effort
  "avg_bat_speed_all", "hard_swing_rate_all", "swing_length_all",
  "squared_up_per_swing_all", "blast_per_swing_all",
  # miss distance by cut
  "miss_distance_all", "miss_distance_bip", "miss_distance_whiff",
  # BIP miss geometry (contact-direction identity)
  "early_percent_bip", "late_percent_bip", "on_time_percent_bip",
  "under_percent_bip", "over_percent_bip", "lined_up_percent_bip",
  "tied_up_percent_bip", "flailed_percent_bip", "centered_percent_bip",
  # whiff miss geometry (vulnerability)
  "under_percent_whiff", "late_percent_whiff", "over_percent_whiff"
)
# OUTCOMES = results (what happens)
DEFAULT_OUTCOMES <- c(
  "xwoba", "woba", "barrel_pct", "hardhit_pct", "ev90", "max_ev", "la", "iso", "slg",
  "gb_pct", "fb_pct", "ld_pct", "pull_air_rate", "pull_gb_rate",
  "pull_pct", "oppo_pct", "k_pct", "bb_pct", "whiff_rate", "hr", "sb"
)


# ── Input -> outcome correlation map ──────────────────────────────────────────
correlation_map <- function(tbl,
                            inputs   = DEFAULT_INPUTS,
                            outcomes = DEFAULT_OUTCOMES,
                            method   = "pearson",     # "pearson" | "spearman"
                            write    = TRUE) {

  inputs   <- intersect(inputs,   names(tbl))
  outcomes <- intersect(outcomes, names(tbl))

  X <- tbl |> select(all_of(inputs))
  Y <- tbl |> select(all_of(outcomes))

  # pairwise-complete correlation, inputs (rows) x outcomes (cols)
  M <- matrix(NA_real_, nrow = length(inputs), ncol = length(outcomes),
              dimnames = list(inputs, outcomes))
  for (i in inputs) for (o in outcomes) {
    M[i, o] <- suppressWarnings(
      cor(X[[i]], Y[[o]], use = "pairwise.complete.obs", method = method))
  }

  if (write) {
    csv_path <- file.path(OUT_DIR, sprintf("correlation_map_%s.csv", method))
    write.csv(round(M, 3), csv_path)
    message("Wrote correlation matrix -> ", csv_path)

    # Heatmap
    long <- expand.grid(input = rownames(M), outcome = colnames(M),
                        stringsAsFactors = FALSE)
    long$r <- mapply(function(i, o) M[i, o], long$input, long$outcome)
    long$input   <- factor(long$input,   levels = rev(inputs))
    long$outcome <- factor(long$outcome, levels = outcomes)

    p <- ggplot(long, aes(outcome, input, fill = r)) +
      geom_tile(color = "grey90") +
      geom_text(aes(label = sprintf("%.2f", r)), size = 2.4) +
      scale_fill_gradient2(low = "#b2182b", mid = "white", high = "#2166ac",
                           midpoint = 0, limits = c(-1, 1), na.value = "grey80") +
      labs(title = sprintf("Input -> Outcome %s correlations (n = %d player-seasons)",
                           tools::toTitleCase(method), nrow(tbl)),
           x = "Outcome", y = "Mechanical input", fill = "r") +
      theme_minimal(base_size = 9) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))

    png_path <- file.path(OUT_DIR, sprintf("correlation_map_%s.png", method))
    ggsave(png_path, p, width = 11, height = 8, dpi = 150)
    message("Wrote heatmap -> ", png_path)
  }

  invisible(M)
}


# ── "Early + Under = pull-air" headline test ──────────────────────────────────
# Hypothesis: hitters who are EARLY (out front) and UNDER (below the ball) on
# balls in play should pull fly balls (high pull_air_rate).
pull_air_test <- function(tbl, n = 15, year = NULL) {

  d <- tbl
  if (!is.null(year)) d <- d |> filter(year == !!year)

  d <- d |> filter(!is.na(early_percent_bip), !is.na(under_percent_bip),
                   !is.na(pull_air_rate))

  # composite score: z(early_bip) + z(under_bip)
  z <- function(x) (x - mean(x, na.rm = TRUE)) / sd(x, na.rm = TRUE)
  d <- d |> mutate(early_under_score = z(early_percent_bip) + z(under_percent_bip))

  # Correlations against the pull-air target (and a contrast: pulled grounders)
  cors <- c(
    early_bip_vs_pullair   = cor(d$early_percent_bip, d$pull_air_rate, use = "pairwise.complete.obs"),
    under_bip_vs_pullair   = cor(d$under_percent_bip, d$pull_air_rate, use = "pairwise.complete.obs"),
    score_vs_pullair       = cor(d$early_under_score, d$pull_air_rate, use = "pairwise.complete.obs"),
    score_vs_pullgb        = cor(d$early_under_score, d$pull_gb_rate,  use = "pairwise.complete.obs"),
    score_vs_barrel        = cor(d$early_under_score, d$barrel_pct,    use = "pairwise.complete.obs"),
    score_vs_la            = cor(d$early_under_score, d$la,            use = "pairwise.complete.obs")
  )

  leaders <- d |>
    arrange(desc(early_under_score)) |>
    transmute(name, year, bat_side,
              early_bip = round(early_percent_bip, 3),
              under_bip = round(under_percent_bip, 3),
              score     = round(early_under_score, 2),
              pull_air  = round(pull_air_rate, 3),
              pull_gb   = round(pull_gb_rate, 3),
              barrel    = round(barrel_pct, 3),
              la        = round(la, 1),
              hr) |>
    head(n)

  pop <- d |> summarise(
    pull_air = round(median(pull_air_rate, na.rm = TRUE), 3),
    barrel   = round(median(barrel_pct, na.rm = TRUE), 3),
    la       = round(median(la, na.rm = TRUE), 1)
  )

  cat("\n══════════════════════════════════════════════════════════════\n")
  cat(" EARLY + UNDER (BIP)  ->  PULL-AIR  test\n")
  cat("══════════════════════════════════════════════════════════════\n")
  cat(sprintf(" n = %d player-seasons%s\n\n", nrow(d),
              if (is.null(year)) " (2024-26 pooled)" else paste0(" (", year, ")")))
  cat("Correlations:\n")
  for (nm in names(cors)) cat(sprintf("  %-24s r = % .3f\n", nm, cors[nm]))
  cat(sprintf("\nPopulation medians:  pull_air = %.3f | barrel = %.3f | LA = %.1f\n",
              pop$pull_air, pop$barrel, pop$la))
  cat(sprintf("\nTop %d early+under hitters (do they pull air?):\n", n))
  print(as.data.frame(leaders), row.names = FALSE)

  invisible(list(correlations = cors, leaders = leaders, pop = pop))
}


# ── CLI runner ───────────────────────────────────────────────────────────────
if (any(grepl("--file=", commandArgs(FALSE)))) {
  tbl <- build_hitter_table()
  correlation_map(tbl, method = "pearson")
  correlation_map(tbl, method = "spearman")
  pull_air_test(tbl)
}
