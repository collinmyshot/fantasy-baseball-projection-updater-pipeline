#!/usr/bin/env Rscript
# ══════════════════════════════════════════════════════════════════════════════
# derive_park_factor_weights.R  —  composite park-factor weights, v1
#
# Finds the blend of the three component park deltas
#   bacon_resid  (batting average on contact)
#   hr_resid     (home runs per batted-ball event)
#   xbh_resid    (extra-base hits)
# that best reconstructs the OVERALL wOBAcon-residual park effect, i.e. the
# target column delta_woba_over_xwoba_overall.
#
# Method: weights are parameterised through a softmax so they are non-negative
# and sum to 1 by construction (no constrained optimiser needed). Fit is
# BBE-weighted at PARK-ERA grain, park eras with fewer than --min-bbe (default
# 1000) batted balls are dropped, and the weights are bootstrapped
# (--n-boot, default 400; --seed 42) to get p05 / median / p95 bands.
#
# ⚠ KNOWN LIMITATION OF THIS TARGET — READ BEFORE QUOTING v1 WEIGHTS.
#   The target is the wOBAcon residual, and bacon_resid is very nearly that
#   same variable. So this derivation GUARANTEES that BACON dominates the
#   blend. It is a self-consistency check, not evidence about what matters
#   for fantasy value, and it says nothing about points.
#   derive_park_factor_weights_v2.R exists specifically to fix this: it
#   regresses the OTTONEU-POINTS park effect on the same three deltas with
#   free coefficients, which is the question you usually actually want.
#   Prefer v2 unless you specifically want the wOBAcon-reconstruction view.
#
# Inputs (all must already exist in --output-dir, else it stops loudly):
#   park_factors_overall.csv, park_factors_bacon_overall.csv,
#   park_factors_hr_overall.csv, park_factors_xbh_overall.csv
#
# Outputs (to --output-dir, default data/processed/park_factors):
#   park_factor_weight_recommendation.csv   weights + bootstrap bands
#   plus a diagnostics table (n park eras, weighted RMSE, weighted corr) and a
#   per-park-era predicted-vs-target table
#
# ⚠ NO VERDICT RECORDED — no stored result for this run, so nothing is asserted
#   here about what weights it returned. Re-run for the numbers.
#
# Usage:
#   Rscript scripts/validation_tuning/derive_park_factor_weights.R
#   Rscript scripts/validation_tuning/derive_park_factor_weights.R \
#       --output-dir data/processed/park_factors --n-boot 400 --min-bbe 1000
# ══════════════════════════════════════════════════════════════════════════════
source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  output_dir = list(flag = "--output-dir", default = file.path("data", "processed", "park_factors")),
  seed       = list(flag = "--seed",       default = 42, type = "numeric"),
  n_boot     = list(flag = "--n-boot",     default = 400, type = "numeric"),
  min_bbe    = list(flag = "--min-bbe",    default = 1000, type = "numeric")
))

output_dir <- parsed$output_dir
seed       <- as.integer(parsed$seed)
n_boot     <- as.integer(parsed$n_boot)
min_bbe    <- as.integer(parsed$min_bbe)

files_needed <- c(
  file.path(output_dir, "park_factors_overall.csv"),
  file.path(output_dir, "park_factors_bacon_overall.csv"),
  file.path(output_dir, "park_factors_hr_overall.csv"),
  file.path(output_dir, "park_factors_xbh_overall.csv")
)
missing <- files_needed[!file.exists(files_needed)]
if (length(missing) > 0) {
  stop(sprintf("Missing required files:\n- %s", paste(missing, collapse = "\n- ")))
}

softmax3 <- function(theta) {
  z <- exp(theta - max(theta))
  z / sum(z)
}

weighted_cor <- function(x, y, w) {
  keep <- is.finite(x) & is.finite(y) & is.finite(w) & w > 0
  if (!any(keep)) {
    return(NA_real_)
  }
  x <- x[keep]
  y <- y[keep]
  w <- w[keep]
  w <- w / sum(w)
  mx <- sum(w * x)
  my <- sum(w * y)
  vx <- sum(w * (x - mx)^2)
  vy <- sum(w * (y - my)^2)
  if (!is.finite(vx) || !is.finite(vy) || vx <= 0 || vy <= 0) {
    return(NA_real_)
  }
  cov_xy <- sum(w * (x - mx) * (y - my))
  cov_xy / sqrt(vx * vy)
}

fit_weights <- function(y, X, obs_w) {
  obj <- function(theta) {
    w <- softmax3(theta)
    pred <- as.numeric(X %*% w)
    sum(obs_w * (y - pred)^2)
  }

  fit <- stats::optim(par = c(0, 0, 0), fn = obj, method = "BFGS", control = list(maxit = 2000))
  w <- softmax3(fit$par)
  names(w) <- c("bacon_resid", "hr_resid", "xbh_resid")
  w
}

overall <- utils::read.csv(file.path(output_dir, "park_factors_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)
bacon <- utils::read.csv(file.path(output_dir, "park_factors_bacon_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)
hr <- utils::read.csv(file.path(output_dir, "park_factors_hr_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)
xbh <- utils::read.csv(file.path(output_dir, "park_factors_xbh_overall.csv"), stringsAsFactors = FALSE, check.names = FALSE)

dat <- merge(
  overall[, c("park_era_id", "delta_woba_over_xwoba_overall", "n_bbe")],
  bacon[, c("park_era_id", "delta_overall")],
  by = "park_era_id",
  all = FALSE
)
names(dat)[names(dat) == "delta_overall"] <- "bacon_resid"

dat <- merge(
  dat,
  hr[, c("park_era_id", "delta_overall")],
  by = "park_era_id",
  all = FALSE
)
names(dat)[names(dat) == "delta_overall"] <- "hr_resid"

dat <- merge(
  dat,
  xbh[, c("park_era_id", "delta_overall")],
  by = "park_era_id",
  all = FALSE
)
names(dat)[names(dat) == "delta_overall"] <- "xbh_resid"

keep <- is.finite(dat$delta_woba_over_xwoba_overall) &
  is.finite(dat$bacon_resid) &
  is.finite(dat$hr_resid) &
  is.finite(dat$xbh_resid) &
  is.finite(dat$n_bbe) &
  dat$n_bbe >= min_bbe
dat <- dat[keep, ]

if (nrow(dat) < 10) {
  stop(sprintf("Not enough park eras after filtering (n=%s).", nrow(dat)))
}

X <- as.matrix(dat[, c("bacon_resid", "hr_resid", "xbh_resid")])
y <- as.numeric(dat$delta_woba_over_xwoba_overall)
obs_w <- as.numeric(dat$n_bbe)

weights_hat <- fit_weights(y, X, obs_w)
pred_hat <- as.numeric(X %*% weights_hat)

rmse_w <- sqrt(sum(obs_w * (y - pred_hat)^2) / sum(obs_w))
corr_w <- weighted_cor(pred_hat, y, obs_w)

set.seed(seed)
boot_w <- matrix(NA_real_, nrow = n_boot, ncol = 3)
colnames(boot_w) <- c("bacon_resid", "hr_resid", "xbh_resid")

for (b in seq_len(n_boot)) {
  idx <- sample.int(nrow(dat), size = nrow(dat), replace = TRUE)
  Xb <- X[idx, , drop = FALSE]
  yb <- y[idx]
  wb <- obs_w[idx]
  boot_w[b, ] <- fit_weights(yb, Xb, wb)
}

boot_df <- as.data.frame(boot_w, stringsAsFactors = FALSE)

qfun <- function(x, p) stats::quantile(x, probs = p, na.rm = TRUE, names = FALSE, type = 7)

summary_tbl <- data.frame(
  component = c("bacon_resid", "hr_resid", "xbh_resid"),
  weight_opt = as.numeric(weights_hat),
  weight_boot_median = c(
    qfun(boot_df$bacon_resid, 0.5),
    qfun(boot_df$hr_resid, 0.5),
    qfun(boot_df$xbh_resid, 0.5)
  ),
  weight_boot_p05 = c(
    qfun(boot_df$bacon_resid, 0.05),
    qfun(boot_df$hr_resid, 0.05),
    qfun(boot_df$xbh_resid, 0.05)
  ),
  weight_boot_p95 = c(
    qfun(boot_df$bacon_resid, 0.95),
    qfun(boot_df$hr_resid, 0.95),
    qfun(boot_df$xbh_resid, 0.95)
  ),
  stringsAsFactors = FALSE
)

diag_tbl <- data.frame(
  n_park_eras = nrow(dat),
  n_boot = n_boot,
  min_bbe_filter = min_bbe,
  weighted_rmse = rmse_w,
  weighted_corr = corr_w,
  stringsAsFactors = FALSE
)

pred_tbl <- data.frame(
  park_era_id = dat$park_era_id,
  target_delta_woba_over_xwoba = y,
  pred_weighted_components = pred_hat,
  n_bbe = obs_w,
  stringsAsFactors = FALSE
)

utils::write.csv(
  summary_tbl,
  file.path(output_dir, "park_factor_weight_recommendation.csv"),
  row.names = FALSE,
  na = ""
)
utils::write.csv(
  diag_tbl,
  file.path(output_dir, "park_factor_weight_diagnostics.csv"),
  row.names = FALSE,
  na = ""
)
utils::write.csv(
  boot_df,
  file.path(output_dir, "park_factor_weight_bootstrap_draws.csv"),
  row.names = FALSE,
  na = ""
)
utils::write.csv(
  pred_tbl,
  file.path(output_dir, "park_factor_weight_fit_table.csv"),
  row.names = FALSE,
  na = ""
)

message("Recommended weights (not applied):")
for (k in seq_len(nrow(summary_tbl))) {
  message(
    sprintf(
      "  %s: opt=%.4f median=%.4f (p05=%.4f, p95=%.4f)",
      summary_tbl$component[k],
      summary_tbl$weight_opt[k],
      summary_tbl$weight_boot_median[k],
      summary_tbl$weight_boot_p05[k],
      summary_tbl$weight_boot_p95[k]
    )
  )
}
message(sprintf("Weighted RMSE=%.6f, weighted corr=%.4f (n=%s park eras).", rmse_w, corr_w, nrow(dat)))
