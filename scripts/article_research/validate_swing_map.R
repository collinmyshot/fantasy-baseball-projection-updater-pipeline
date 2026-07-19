#!/usr/bin/env Rscript
# validate_swing_map.R  — Phase A robustness pass
# Addresses: (1) per-year replication of headline relationships,
#            (2) handedness as a confound for the timing->spray signal,
#            (3) proper regression (de-cherry-pick the in-sample 0.45),
#            (4) per-BBE vs per-PA denominators (the Gallo problem).
#
# NOTE: this does NOT implement the in-season "% of maximal predictiveness"
# stabilization — that needs (a) monthly 2025 data and (b) the exact threshold
# definition, both pending user input.

suppressPackageStartupMessages({library(readr); library(dplyr)})

tbl <- read_csv("data/processed/swing_map/hitter_swing_map.csv", show_col_types = FALSE)

# Per-PA reconstructions (fix BBE-denominated outcomes)
tbl <- tbl |>
  mutate(
    barrel_per_pa   = barrel_pct * events  / pa,    # FG barrels / PA
    pull_air_per_pa = pull_air_rate * sv_bbe / pa,   # Savant pulled-air balls / PA
    bbe_per_pa      = events / pa                     # contact/BIP density (Gallo axis)
  )

cr <- function(d, a, b) suppressWarnings(cor(d[[a]], d[[b]], use = "pairwise.complete.obs"))

cat("################################################################\n")
cat("# A1. PER-YEAR REPLICATION of headline relationships\n")
cat("#     (stable signal repeats across years; noise doesn't)\n")
cat("################################################################\n")
pairs <- tribble(
  ~input,                      ~outcome,
  "avg_bat_speed_all",         "ev90",
  "blast_per_swing_all",       "hardhit_pct",
  "squared_up_per_swing_all",  "k_pct",
  "miss_distance_all",         "whiff_rate",
  "early_percent_all",         "pull_pct",
  "late_percent_all",          "oppo_pct",
  "under_percent_all",         "fb_pct",
  "swing_length_all",          "pull_air_rate"
)
cat(sprintf("\n%-26s %-14s %8s %8s %8s %8s\n", "input", "outcome", "2024", "2025", "2026", "pooled"))
for (i in seq_len(nrow(pairs))) {
  ii <- pairs$input[i]; oo <- pairs$outcome[i]
  rr <- sapply(c(2024, 2025, 2026), function(y) cr(filter(tbl, year == y), ii, oo))
  rp <- cr(tbl, ii, oo)
  cat(sprintf("%-26s %-14s %8.2f %8.2f %8.2f %8.2f\n", ii, oo, rr[1], rr[2], rr[3], rp))
}

cat("\n################################################################\n")
cat("# A2. HANDEDNESS: does early->pull hold within L and R?\n")
cat("################################################################\n")
for (hand in c("L", "R")) {
  d <- tbl |> filter(bat_side == hand)
  cat(sprintf("  %s (n=%d):  early_all->pull = % .3f | late_all->oppo = % .3f | under_all->fb = % .3f\n",
              hand, nrow(d), cr(d, "early_percent_all", "pull_pct"),
              cr(d, "late_percent_all", "oppo_pct"), cr(d, "under_percent_all", "fb_pct")))
}

cat("\n################################################################\n")
cat("# A3. REGRESSION for pull-air (de-cherry-pick the 0.45)\n")
cat("#     in-sample R2 vs out-of-sample (train 24-25, test 26)\n")
cat("################################################################\n")
predictors <- c("avg_bat_speed_all", "hard_swing_rate_all", "swing_length_all",
                "squared_up_per_swing_all", "early_percent_all", "late_percent_all",
                "under_percent_all", "over_percent_all", "tied_up_percent_all",
                "flailed_percent_all", "contact_pct")   # contact_pct = denominator control

run_reg <- function(target) {
  d <- tbl |> select(year, all_of(c(target, predictors))) |> na.omit()
  f <- as.formula(paste(target, "~", paste(predictors, collapse = " + ")))
  # pooled fit
  m <- lm(f, data = d)
  r2 <- summary(m)$r.squared; ar2 <- summary(m)$adj.r.squared
  # out-of-sample: train 2024-25, test 2026
  tr <- d |> filter(year %in% c(2024, 2025)); te <- d |> filter(year == 2026)
  oos <- NA
  if (nrow(te) > 10) {
    mt <- lm(f, data = tr)
    pred <- predict(mt, te)
    oos <- cor(pred, te[[target]])^2
  }
  cat(sprintf("\n  %s:  pooled R2 = %.3f (adj %.3f) | OUT-OF-SAMPLE R2 (24-25 -> 26) = %.3f  [n=%d]\n",
              target, r2, ar2, oos, nrow(d)))
  # standardized betas (top drivers)
  ds <- d |> mutate(across(all_of(c(target, predictors)), \(x) as.numeric(scale(x))))
  ms <- lm(f, data = ds)
  co <- sort(coef(ms)[-1], decreasing = TRUE)
  cat("    top + drivers: ", paste(sprintf("%s(%.2f)", names(head(co,3)), head(co,3)), collapse=", "), "\n")
  cat("    top - drivers: ", paste(sprintf("%s(%.2f)", names(tail(co,3)), tail(co,3)), collapse=", "), "\n")
}
for (t in c("pull_air_rate", "pull_air_per_pa", "xwoba", "k_pct")) run_reg(t)

cat("\n################################################################\n")
cat("# A4. DENOMINATOR (Gallo problem): per-BBE vs per-PA\n")
cat("################################################################\n")
d25 <- tbl |> filter(year == 2025, pa >= 300)
cat(sprintf("\n  cor(barrel_per_BBE, barrel_per_PA) = %.3f  (if <1, contact rate reshuffles)\n",
            cr(d25, "barrel_pct", "barrel_per_pa")))
cat(sprintf("  cor(contact_pct, barrel_per_BBE)   = % .3f\n", cr(d25, "contact_pct", "barrel_pct")))
cat(sprintf("  cor(contact_pct, barrel_per_PA)    = % .3f   (per-PA should reward contact more)\n", cr(d25, "contact_pct", "barrel_per_pa")))
cat("\n  Biggest per-BBE -> per-PA FALLERS (low-contact mashers, the Gallo type):\n")
d25 |>
  filter(!is.na(barrel_per_pa)) |>
  mutate(rank_bbe = rank(-barrel_pct), rank_pa = rank(-barrel_per_pa),
         drop = rank_pa - rank_bbe) |>
  arrange(desc(drop)) |>
  transmute(name, contact = round(contact_pct, 3),
            barrel_bbe = round(barrel_pct, 3), barrel_pa = round(barrel_per_pa, 3),
            rank_bbe = round(rank_bbe), rank_pa = round(rank_pa), rank_drop = round(drop)) |>
  head(8) |> as.data.frame() |> print(row.names = FALSE)
