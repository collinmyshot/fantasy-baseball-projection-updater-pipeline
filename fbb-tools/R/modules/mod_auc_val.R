suppressPackageStartupMessages({
  library(DT)
  library(openxlsx)
})

# ── Constants ──────────────────────────────────────────────────────────────────

# Roto category definitions
AUC_H_ROTO_CATS    <- c("pa", "r", "hr", "rbi", "sb", "avg", "obp")
AUC_H_ROTO_LABS    <- c("PA", "R", "HR", "RBI", "SB", "AVG", "OBP")
AUC_H_RATE_CATS    <- c("avg", "obp")   # not scaled per PA
AUC_H_DISPLAY_ONLY <- c("pa")          # always ticked; shown in table but never z-scored

AUC_P_ROTO_CATS    <- c("ip", "w", "k", "sv", "hd", "svhd", "era", "whip")
AUC_P_DISPLAY_ONLY <- c("ip")          # always ticked; shown in table but never z-scored
AUC_P_ROTO_LABS <- c("IP", "W", "K", "SV", "HD", "SVHD", "ERA", "WHIP")
AUC_P_RATE_CATS <- c("era", "whip")  # z-score negated (lower = better)
AUC_P_COUNT_CATS <- c("w", "k", "sv", "hd", "svhd")  # scaled per IP

# Default cat weights (1 = equal for all; used by "Equal Weights" preset)
AUC_H_CAT_WT_DEFAULT <- setNames(rep(1, length(AUC_H_ROTO_CATS)), AUC_H_ROTO_CATS)
AUC_P_CAT_WT_DEFAULT <- setNames(rep(1, length(AUC_P_ROTO_CATS)), AUC_P_ROTO_CATS)

# CK hitting category weights (source: fantasy-baseball-projections app)
AUC_H_CAT_WT_CK <- c(r = 0.6, hr = 1.35, rbi = 0.6, sb = 1, avg = 1, obp = 1)

# Roto preset category selections
AUC_ROTO_PRESETS <- list(
  "Standard 5x5" = list(h = c("r", "hr", "rbi", "sb", "avg"),
                         p = c("w", "k", "sv", "era", "whip")),
  "5x5 OBP"      = list(h = c("r", "hr", "rbi", "sb", "obp"),
                         p = c("w", "k", "sv", "era", "whip")),
  "5x5 SVHD"     = list(h = c("r", "hr", "rbi", "sb", "avg"),
                         p = c("w", "k", "svhd", "era", "whip"))
)

# Points presets — named vector of (stat = point_value) per side
AUC_POINTS_PRESETS <- list(
  "Ottoneu FG Points" = list(
    h = c(ab = -1.0, h = 5.6, x1b = 0, x2b = 2.9, x3b = 5.7, hr = 9.4,
          r = 0, rbi = 0, bb = 3.0, hbp = 3.0, sb = 1.9, cs = -2.8),
    p = c(ip = 7.4, w = 0, k = 2.0, h_allow = -2.6, bb_allow = -3.0,
          hbp_bat = -3.0, hr_allow = -12.3, er = 0, qs = 0, sv = 5.0,
          hd = 4.0, bs = 0)
  ),
  "Danny League" = list(
    h = c(ab = -0.5, h = 0, x1b = 1.5, x2b = 2.5, x3b = 3.5, hr = 6.5,
          r = 1.5, rbi = 1.5, bb = 1.0, hbp = 0, sb = 4.0, cs = 0),
    p = c(ip = 1.5, w = 4.0, k = 1.5, h_allow = -0.5, bb_allow = -0.3,
          hbp_bat = 0, hr_allow = 0, er = -3.0, qs = 8.0, sv = 8.0,
          hd = 6.0, bs = -2.0)
  )
)
AUC_POINTS_PRESET_NAMES <- names(AUC_POINTS_PRESETS)

# Points stat display labels (for the numeric input grid)
AUC_H_PTS_STAT_LABS  <- c(ab = "AB", h = "H", x1b = "1B", x2b = "2B", x3b = "3B",
                            hr = "HR", r = "R", rbi = "RBI", bb = "BB", hbp = "HBP",
                            sb = "SB", cs = "CS")
AUC_P_PTS_STAT_LABS  <- c(ip = "IP", w = "W", k = "K", h_allow = "H (allowed)",
                            bb_allow = "BB (issued)", hbp_bat = "HBP (batters)",
                            hr_allow = "HR (allowed)", er = "ER", qs = "QS",
                            sv = "SV", hd = "HD", bs = "BS")

# SP classification thresholds
AUC_SP_IP_MIN   <- 50.0
AUC_SP_GS_RATIO <- 0.50   # GS/G >= this → SP
AUC_SP_GS_MIN   <- 5L     # GS < this AND ratio below threshold → pure RP; else SP/RP

# League defaults
AUC_DEFAULT_TEAMS      <- 12L
AUC_DEFAULT_BUDGET     <- 260
AUC_DEFAULT_START_BATS <- 14L
AUC_DEFAULT_START_SP   <- 7L
AUC_DEFAULT_HIT_PCT    <- 70

# Extended FG column candidates for auction (superset of aggregator's lists)
AUC_H_COL_CANDIDATES <- list(
  playerid = c("playerid", "playeridfg", "fgplayerid", "idfg", "id"),
  name     = c("playername", "name", "shortname"),
  team     = c("team", "tm"),
  g        = c("g", "games", "gp"),
  pa       = c("pa", "plateappearances"),
  ab       = c("ab", "atbats"),
  h        = c("h", "hits"),
  x2b      = c("2b", "doubles", "x2b"),
  x3b      = c("3b", "triples", "x3b"),
  bb       = c("bb", "walks"),
  hbp      = c("hbp", "hitbypitch"),
  r        = c("r", "runs"),
  hr       = c("hr", "homeruns"),
  rbi      = c("rbi", "runsbattedin"),
  sb       = c("sb", "stolenbases"),
  cs       = c("cs", "caughtstealing"),
  avg      = c("avg", "battingaverage")
)

AUC_P_COL_CANDIDATES <- list(
  playerid = c("playerid", "playeridfg", "fgplayerid", "idfg", "id"),
  name     = c("playername", "name", "shortname"),
  team     = c("team", "tm"),
  g        = c("g", "games", "gp"),
  gs       = c("gs", "gamesstarted"),
  ip       = c("ip", "inningspitched"),
  w        = c("w", "wins"),
  sv       = c("sv", "saves"),
  hd       = c("hld", "hd", "holds", "hold"),
  k        = c("so", "k", "strikeouts", "ks"),
  h_allow  = c("h", "hits"),
  bb_allow = c("bb", "walks"),
  hbp_bat  = c("hbp", "hitbypitch"),
  hr_allow = c("hr", "homeruns"),
  er       = c("er", "earnedruns"),
  qs       = c("qs", "qualitystarts"),
  bs       = c("bs", "bsv", "blownsaves"),
  era      = c("era", "earnedruntaverage"),
  whip     = c("whip")
)

# ── Shared helpers (reuse aggregator's global functions) ──────────────────────
# normalize_col(), fg_select_col(), proj_fg_fetch(), build_fg_url(),
# as_df(), compute_aggregate(), PROJ_SYSTEMS, PROJ_KEYS, PROJ_LABELS,
# PROJ_COLLIN_WT_H, PROJ_COLLIN_WT_P, PROJ_EVEN_WT, FG_API_TYPES
# are all available globally from mod_proj_agg.R being sourced first.

# ── Extended FG fetch (used by auction inline build) ──────────────────────────

auc_fetch_fg_projections <- function(key, stats_type, api_types_map = FG_API_TYPES) {
  col_map   <- if (stats_type == "bat") AUC_H_COL_CANDIDATES else AUC_P_COL_CANDIDATES
  api_types <- api_types_map[[key]]
  if (is.null(api_types)) api_types <- key

  for (api_type in api_types) {
    url     <- build_fg_url(api_type, stats_type)
    payload <- proj_fg_fetch(url)
    raw     <- as_df(payload)
    if (is.null(raw) || nrow(raw) == 0) next

    out <- data.frame(
      playerid = as.character(fg_select_col(raw, col_map$playerid)),
      name     = as.character(fg_select_col(raw, col_map$name)),
      team     = as.character(fg_select_col(raw, col_map$team)),
      stringsAsFactors = FALSE
    )

    stat_cols <- setdiff(names(col_map), c("playerid", "name", "team"))
    for (s in stat_cols) {
      out[[s]] <- suppressWarnings(as.numeric(fg_select_col(raw, col_map[[s]])))
    }

    if (stats_type == "bat") {
      # OBP fallback
      if (!"obp" %in% names(out) || all(is.na(out$obp))) {
        pa <- out$pa; h <- out$h; bb <- out$bb
        if (!all(is.na(pa)) && !all(is.na(h)) && !all(is.na(bb)))
          out$obp <- ifelse(!is.na(pa) & pa > 0, (h + bb) / pa, NA_real_)
      }
    }

    if (stats_type == "pit") {
      sv <- if ("sv" %in% names(out)) out$sv else NA_real_
      hd <- if ("hd" %in% names(out)) out$hd else NA_real_
      out$svhd <- ifelse(
        is.na(sv) & is.na(hd), NA_real_,
        rowSums(cbind(ifelse(is.na(sv), 0, sv), ifelse(is.na(hd), 0, hd)))
      )
    }

    out <- out[!is.na(out$playerid) & nzchar(out$playerid), ]
    if (nrow(out) > 0) return(out)
  }
  NULL
}

# ── YTD stats fetch + merge ──────────────────────────────────────────────────

auc_fetch_ytd <- function(stats_type) {
  yr  <- as.integer(format(Sys.Date(), "%Y"))
  url <- paste0(
    "https://www.fangraphs.com/api/leaders/major-league/data",
    "?pos=all&stats=", stats_type,
    "&lg=all&ind=0&team=0&rost=0&players=0&qual=0&type=0&month=0",
    "&season=", yr, "&season1=", yr,
    "&pageitems=2000&pagenum=1"
  )
  col_map <- if (stats_type == "bat") AUC_H_COL_CANDIDATES else AUC_P_COL_CANDIDATES

  result  <- fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
  payload <- if (isTRUE(result$ok)) result$payload else NULL

  raw <- as_df(payload)
  if (is.null(raw) || nrow(raw) == 0) return(NULL)
  out <- data.frame(
    playerid = as.character(fg_select_col(raw, col_map$playerid)),
    name     = trimws(gsub("<[^>]+>", "", as.character(fg_select_col(raw, col_map$name)))),
    team     = trimws(gsub("<[^>]+>", "", as.character(fg_select_col(raw, col_map$team)))),
    stringsAsFactors = FALSE
  )
  for (s in setdiff(names(col_map), c("playerid","name","team")))
    out[[s]] <- suppressWarnings(as.numeric(fg_select_col(raw, col_map[[s]])))
  if (stats_type == "bat") {
    if (all(c("h","x2b","x3b","hr") %in% names(out))) {
      out$x1b <- pmax(0, out$h - out$x2b - out$x3b - out$hr, na.rm = FALSE)
      out$x1b[is.na(out$h)|is.na(out$x2b)|is.na(out$x3b)|is.na(out$hr)] <- NA_real_
    }
    if (!("obp" %in% names(out)) || all(is.na(out$obp))) {
      hbp_v <- if ("hbp" %in% names(out)) out$hbp else rep(0, nrow(out))
      hbp_v[is.na(hbp_v)] <- 0
      out$obp <- ifelse(!is.na(out$pa) & out$pa > 0,
                        (out$h + out$bb + hbp_v) / out$pa, NA_real_)
    }
  } else {
    sv <- if ("sv" %in% names(out)) out$sv else NA_real_
    hd <- if ("hd" %in% names(out)) out$hd else NA_real_
    out$svhd <- ifelse(is.na(sv) & is.na(hd), NA_real_,
                       rowSums(cbind(ifelse(is.na(sv),0,sv), ifelse(is.na(hd),0,hd))))
    if ((!("er" %in% names(out)) || all(is.na(out$er))) && all(c("era","ip") %in% names(out)))
      out$er <- out$era * out$ip / 9
  }
  out <- out[!is.na(out$playerid) & nzchar(out$playerid), ]
  if (nrow(out) > 0) out else NULL
}

.ytd_sum2 <- function(a, b) { a[is.na(a)] <- 0; b[is.na(b)] <- 0; a + b }

auc_merge_ytd_ros_h <- function(ros, ytd) {
  if (is.null(ytd) || nrow(ytd) == 0) return(ros)
  ros_id <- tolower(trimws(as.character(ros$playerid)))
  ytd_id <- tolower(trimws(as.character(ytd$playerid)))
  idx    <- match(ros_id, ytd_id)
  unm <- which(is.na(idx))
  if (length(unm)) idx[unm] <- match(tolower(trimws(ros$name[unm])), tolower(trimws(ytd$name)))
  has <- !is.na(idx)
  for (s in c("pa","ab","h","x1b","x2b","x3b","hr","r","rbi","bb","hbp","sb","cs","g")) {
    if (!s %in% names(ros)) ros[[s]] <- NA_real_
    yc <- if (s %in% names(ytd)) ytd[[s]] else rep(NA_real_, nrow(ytd))
    ros[[s]][has] <- .ytd_sum2(ros[[s]][has], yc[idx[has]])
  }
  hbp_v <- if ("hbp" %in% names(ros)) ros$hbp else rep(0, nrow(ros)); hbp_v[is.na(hbp_v)] <- 0
  ros$avg <- ifelse(!is.na(ros$ab) & ros$ab > 0, ros$h / ros$ab, NA_real_)
  ros$obp <- ifelse(!is.na(ros$pa) & ros$pa > 0, (ros$h + ros$bb + hbp_v) / ros$pa, NA_real_)
  ytd_only <- ytd[setdiff(seq_len(nrow(ytd)), idx[has]), , drop = FALSE]
  if (nrow(ytd_only) > 0) {
    hv <- if ("hbp" %in% names(ytd_only)) ytd_only$hbp else rep(0, nrow(ytd_only)); hv[is.na(hv)] <- 0
    ytd_only$avg <- ifelse(!is.na(ytd_only$ab) & ytd_only$ab > 0,
                           ytd_only$h / ytd_only$ab, NA_real_)
    ytd_only$obp <- ifelse(!is.na(ytd_only$pa) & ytd_only$pa > 0,
                           (ytd_only$h + ytd_only$bb + hv) / ytd_only$pa, NA_real_)
    for (col in setdiff(names(ros), names(ytd_only))) ytd_only[[col]] <- NA_real_
    ros <- rbind(ros, ytd_only[, names(ros), drop = FALSE])
  }
  ros
}

auc_merge_ytd_ros_p <- function(ros, ytd) {
  if (is.null(ytd) || nrow(ytd) == 0) return(ros)
  ros_id <- tolower(trimws(as.character(ros$playerid)))
  ytd_id <- tolower(trimws(as.character(ytd$playerid)))
  idx    <- match(ros_id, ytd_id)
  unm <- which(is.na(idx))
  if (length(unm)) idx[unm] <- match(tolower(trimws(ros$name[unm])), tolower(trimws(ytd$name)))
  has <- !is.na(idx)
  for (s in c("ip","w","k","sv","hd","svhd","h_allow","bb_allow","hbp_bat","hr_allow","er","qs","bs","g","gs")) {
    if (!s %in% names(ros)) ros[[s]] <- NA_real_
    yc <- if (s %in% names(ytd)) ytd[[s]] else rep(NA_real_, nrow(ytd))
    ros[[s]][has] <- .ytd_sum2(ros[[s]][has], yc[idx[has]])
  }
  if (all(c("er","ip") %in% names(ros)))
    ros$era  <- ifelse(!is.na(ros$ip) & ros$ip > 0 & !is.na(ros$er), ros$er / ros$ip * 9, ros$era)
  if (all(c("h_allow","bb_allow","ip") %in% names(ros)))
    ros$whip <- ifelse(!is.na(ros$ip) & ros$ip > 0,
                       (ifelse(is.na(ros$h_allow),0,ros$h_allow) + ifelse(is.na(ros$bb_allow),0,ros$bb_allow)) / ros$ip,
                       ros$whip)
  ros$role <- ifelse(classify_sp(ros), "SP", "RP")
  ytd_only <- ytd[setdiff(seq_len(nrow(ytd)), idx[has]), , drop = FALSE]
  if (nrow(ytd_only) > 0) {
    if ((!("er" %in% names(ytd_only)) || all(is.na(ytd_only$er))) && all(c("era","ip") %in% names(ytd_only)))
      ytd_only$er <- ytd_only$era * ytd_only$ip / 9
    ytd_only$role <- ifelse(classify_sp(ytd_only), "SP", "RP")
    for (col in setdiff(names(ros), names(ytd_only))) ytd_only[[col]] <- NA_real_
    ros <- rbind(ros, ytd_only[, names(ros), drop = FALSE])
  }
  ros
}

# ── Pitcher role classification ────────────────────────────────────────────────
# Returns "SP", "RP", or "SP/RP" for each row.
#   SP    : GS/G >= AUC_SP_GS_RATIO (>=50%)
#   RP    : GS/G < ratio AND GS < AUC_SP_GS_MIN (<5 starts) → pure reliever
#   SP/RP : GS/G < ratio AND GS >= AUC_SP_GS_MIN → two-way / swing man
# Falls back to IP threshold when GS/G data unavailable.

classify_role <- function(df) {
  gs <- if ("gs" %in% names(df)) df$gs else NULL
  g  <- if ("g"  %in% names(df)) df$g  else NULL
  if (!is.null(gs) && !is.null(g)) {
    ratio  <- ifelse(!is.na(gs) & !is.na(g) & g > 0, gs / pmax(g, 1), NA_real_)
    is_sp  <- !is.na(ratio) & ratio >= AUC_SP_GS_RATIO
    is_rp  <- !is.na(ratio) & ratio < AUC_SP_GS_RATIO & !is.na(gs) & gs < AUC_SP_GS_MIN
    ifelse(is_sp, "SP", ifelse(is_rp, "RP", "SP/RP"))
  } else {
    ip <- if ("ip" %in% names(df)) df$ip else NA_real_
    ifelse(!is.na(ip) & ip >= AUC_SP_IP_MIN, "SP", "RP")
  }
}

# For the roto z-scoring pipeline: exclude pure relievers ("RP") from scoring pool.
classify_sp <- function(df) classify_role(df) != "RP"

# ── Roto math ─────────────────────────────────────────────────────────────────

z_score_vec <- function(x) {
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) return(out)
  mu <- mean(x[keep]); sd <- stats::sd(x[keep])
  if (is.na(sd) || sd == 0) { out[keep] <- 0; return(out) }
  out[keep] <- (x[keep] - mu) / sd
  out
}

z_from_ref <- function(x, mu, sd) {
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) return(out)
  if (is.na(sd) || sd == 0) { out[keep] <- 0; return(out) }
  out[keep] <- (x[keep] - mu) / sd
  out
}

compute_roto_h_zscores <- function(agg_h, selected_cats, pa_min, starter_count,
                                   cat_weights = NULL, pa_weight = 0) {
  # Filter to PA floor
  df <- agg_h[!is.na(agg_h$pa) & agg_h$pa >= pa_min, , drop = FALSE]
  if (nrow(df) == 0) return(NULL)

  # Strip display-only cats (PA) — shown in table but never z-scored
  scoring_cats <- setdiff(selected_cats, AUC_H_DISPLAY_ONLY)

  # Per-PA rates for ratio cats: H/PA for AVG, (H+BB)/PA for OBP.
  # Consistent with the per-PA framework used for all counting cats. The aggregate was
  # built on per-PA rates scaled by a single reference PA, so z-scoring H/PA keeps the
  # decomposition clean: rate quality captured here, volume captured via z_PA weight.
  if ("avg" %in% scoring_cats && "h" %in% names(df)) {
    df[["avg_src_vol"]] <- ifelse(df$pa > 0 & !is.na(df$h), df$h / df$pa, NA_real_)
  }
  if ("obp" %in% scoring_cats) {
    h_v  <- if ("h"  %in% names(df)) df$h  else rep(0, nrow(df))
    bb_v <- if ("bb" %in% names(df)) df$bb else rep(0, nrow(df))
    df[["obp_src_vol"]] <- ifelse(df$pa > 0, (h_v + bb_v) / df$pa, NA_real_)
  }

  # Per-PA rates for counting cats
  for (s in setdiff(scoring_cats, AUC_H_RATE_CATS)) {
    rate_col <- paste0(s, "_per_pa")
    if (s == "sb") {
      # log1p transform for SB
      df[[rate_col]] <- ifelse(df$pa > 0, log1p(df[[s]] / df$pa), NA_real_)
    } else if (s %in% names(df)) {
      df[[rate_col]] <- ifelse(df$pa > 0, df[[s]] / df$pa, NA_real_)
    }
  }

  # z-scores (scoring cats only — display-only cats like PA are skipped)
  z_cols <- character(0)
  for (s in scoring_cats) {
    z_col  <- paste0("z_", s)
    src <- if (s == "avg" && "avg_src_vol" %in% names(df)) "avg_src_vol"
           else if (s == "obp" && "obp_src_vol" %in% names(df)) "obp_src_vol"
           else if (s %in% AUC_H_RATE_CATS) s
           else paste0(s, "_per_pa")
    if (src %in% names(df)) {
      df[[z_col]] <- z_score_vec(df[[src]])
    } else {
      df[[z_col]] <- NA_real_
    }
    z_cols <- c(z_cols, z_col)
  }

  # Build per-cat weight vector (default 1 for any missing)
  wt_h <- vapply(scoring_cats, function(s) {
    w <- if (!is.null(cat_weights)) cat_weights[[s]] else NULL
    if (is.null(w) || is.na(w)) 1.0 else max(0, w)
  }, numeric(1))
  z_mat  <- as.matrix(df[, z_cols, drop = FALSE])
  all_na <- apply(z_mat, 1, function(r) all(is.na(r)))
  z_mat0 <- z_mat; z_mat0[is.na(z_mat0)] <- 0
  df$z_total <- as.numeric(z_mat0 %*% wt_h)
  # Additive PA volume z-score
  pa_weight <- suppressWarnings(as.numeric(pa_weight))
  if (!is.finite(pa_weight)) pa_weight <- 0
  if (pa_weight > 0) {
    z_pa_full <- z_score_vec(df$pa)
    z_pa_full[is.na(z_pa_full)] <- 0
    df$z_total <- df$z_total + z_pa_full * pa_weight
  }
  df$z_total[all_na] <- NA_real_

  # Starter pool = top N by z_total
  n_start <- min(as.integer(starter_count), sum(!is.na(df$z_total)))
  ranked  <- df[order(df$z_total, decreasing = TRUE, na.last = TRUE), ]
  starters <- ranked[seq_len(n_start), , drop = FALSE]

  # Starter-baseline z-scores (scoring cats only) — collect means/SDs for hypothetical reuse
  stat_means <- numeric(0)
  stat_sds   <- numeric(0)
  for (s in scoring_cats) {
    src <- if (s == "avg" && "avg_src_vol" %in% names(df)) "avg_src_vol"
           else if (s == "obp" && "obp_src_vol" %in% names(df)) "obp_src_vol"
           else if (s %in% AUC_H_RATE_CATS) s
           else paste0(s, "_per_pa")
    if (!src %in% names(starters)) next
    mu <- mean(starters[[src]], na.rm = TRUE)
    sd <- stats::sd(starters[[src]], na.rm = TRUE)
    stat_means[[s]] <- mu
    stat_sds[[s]]   <- sd
    df[[paste0("z_", s, "_s")]] <- z_from_ref(df[[src]], mu, sd)
  }

  z_s_cols <- paste0("z_", scoring_cats, "_s")
  avail_s  <- z_s_cols[z_s_cols %in% names(df)]
  wt_s     <- wt_h[match(sub("z_(.+)_s", "\\1", avail_s), scoring_cats)]
  z_mat_s  <- as.matrix(df[, avail_s, drop = FALSE])
  z_mat_s0 <- z_mat_s; z_mat_s0[is.na(z_mat_s0)] <- 0
  df$z_total_s <- as.numeric(z_mat_s0 %*% wt_s)
  # Additive PA volume z-score (starter-pool baseline; stored for hypothetical reuse)
  pa_mu <- NA_real_; pa_sd_val <- NA_real_
  if (pa_weight > 0) {
    pa_mu     <- mean(starters$pa, na.rm = TRUE)
    pa_sd_val <- stats::sd(starters$pa, na.rm = TRUE)
    if (is.finite(pa_sd_val) && pa_sd_val > 0) {
      z_pa_s <- z_from_ref(df$pa, pa_mu, pa_sd_val)
      z_pa_s[is.na(z_pa_s)] <- 0
      df$z_total_s <- df$z_total_s + z_pa_s * pa_weight
    }
  }
  df$z_total_s[apply(z_mat_s, 1, function(r) all(is.na(r)))] <- NA_real_

  # Refresh starters with z_total_s
  starters <- df[rownames(df) %in% rownames(starters), ]

  list(
    scores     = df,
    starters   = starters,
    n_starters = n_start,
    params     = list(
      means         = stat_means,
      sds           = stat_sds,
      selected_cats = scoring_cats,
      is_rate       = AUC_H_RATE_CATS,
      vol_cats      = intersect(c("avg", "obp"), scoring_cats),
      is_sb         = "sb" %in% scoring_cats,
      cat_weights   = cat_weights,
      pa_weight     = pa_weight,
      pa_mean       = pa_mu,
      pa_sd         = pa_sd_val
    )
  )
}

compute_roto_p_zscores <- function(agg_p, selected_cats, ip_min, starter_count,
                                   cat_weights = NULL, ip_weight = 0) {
  # Classify SP, drop RP entirely
  df <- agg_p
  df$.is_sp <- classify_sp(df)
  sp <- df[df$.is_sp, , drop = FALSE]
  sp <- sp[!is.na(sp$ip) & sp$ip >= ip_min, , drop = FALSE]
  if (nrow(sp) == 0) return(NULL)
  sp$.is_sp <- NULL

  # Strip display-only cats (IP) — shown in table but never z-scored
  scoring_cats <- setdiff(selected_cats, AUC_P_DISPLAY_ONLY)

  # Per-IP rates for ratio cats: ER/IP for ERA, (H_allow+BB_allow)/IP for WHIP.
  # Consistent with the per-IP framework used for all counting cats. ER/IP = ERA/9 exactly;
  # using raw ER when available, falling back to ERA/9 if not.
  if ("era" %in% scoring_cats) {
    if ("er" %in% names(sp) && !all(is.na(sp$er))) {
      sp[["era_src_vol"]] <- ifelse(sp$ip > 0 & !is.na(sp$er), sp$er / sp$ip, NA_real_)
    } else if ("era" %in% names(sp)) {
      sp[["era_src_vol"]] <- sp$era / 9
    }
  }
  if ("whip" %in% scoring_cats) {
    h_v  <- if ("h_allow"  %in% names(sp)) sp$h_allow  else rep(0, nrow(sp))
    bb_v <- if ("bb_allow" %in% names(sp)) sp$bb_allow else rep(0, nrow(sp))
    sp[["whip_src_vol"]] <- ifelse(sp$ip > 0, (h_v + bb_v) / sp$ip, NA_real_)
  }

  # Per-IP rates for counting cats; rate stats used raw
  for (s in scoring_cats) {
    if (s %in% AUC_P_COUNT_CATS && s %in% names(sp)) {
      sp[[paste0(s, "_per_ip")]] <- ifelse(sp$ip > 0, sp[[s]] / sp$ip, NA_real_)
    }
  }

  # z-scores (rate stats negated; display-only cats like IP skipped)
  z_cols <- character(0)
  for (s in scoring_cats) {
    z_col <- paste0("z_", s)
    src <- if (s == "era" && "era_src_vol" %in% names(sp)) "era_src_vol"
           else if (s == "whip" && "whip_src_vol" %in% names(sp)) "whip_src_vol"
           else if (s %in% AUC_P_COUNT_CATS) paste0(s, "_per_ip")
           else s
    if (!src %in% names(sp)) { sp[[z_col]] <- NA_real_; z_cols <- c(z_cols, z_col); next }
    z <- z_score_vec(sp[[src]])
    sp[[z_col]] <- if (s %in% AUC_P_RATE_CATS) -z else z
    z_cols <- c(z_cols, z_col)
  }

  wt_p <- vapply(scoring_cats, function(s) {
    w <- if (!is.null(cat_weights)) cat_weights[[s]] else NULL
    if (is.null(w) || is.na(w)) 1.0 else max(0, w)
  }, numeric(1))
  z_mat_p  <- as.matrix(sp[, z_cols, drop = FALSE])
  all_na   <- apply(z_mat_p, 1, function(r) all(is.na(r)))
  z_mat_p0 <- z_mat_p; z_mat_p0[is.na(z_mat_p0)] <- 0
  sp$z_total <- as.numeric(z_mat_p0 %*% wt_p)
  # Additive IP volume z-score
  ip_weight <- suppressWarnings(as.numeric(ip_weight))
  if (!is.finite(ip_weight)) ip_weight <- 0
  if (ip_weight > 0) {
    z_ip_full <- z_score_vec(sp$ip)
    z_ip_full[is.na(z_ip_full)] <- 0
    sp$z_total <- sp$z_total + z_ip_full * ip_weight
  }
  sp$z_total[all_na] <- NA_real_

  # Starter pool
  n_start <- min(as.integer(starter_count), sum(!is.na(sp$z_total)))
  ranked  <- sp[order(sp$z_total, decreasing = TRUE, na.last = TRUE), ]
  starters <- ranked[seq_len(n_start), , drop = FALSE]

  # Starter-baseline z-scores (scoring cats only) — collect means/SDs for hypothetical reuse
  stat_means <- numeric(0)
  stat_sds   <- numeric(0)
  for (s in scoring_cats) {
    src <- if (s == "era" && "era_src_vol" %in% names(sp)) "era_src_vol"
           else if (s == "whip" && "whip_src_vol" %in% names(sp)) "whip_src_vol"
           else if (s %in% AUC_P_COUNT_CATS) paste0(s, "_per_ip")
           else s
    if (!src %in% names(starters)) next
    mu <- mean(starters[[src]], na.rm = TRUE)
    sd <- stats::sd(starters[[src]], na.rm = TRUE)
    stat_means[[s]] <- mu
    stat_sds[[s]]   <- sd
    z  <- z_from_ref(sp[[src]], mu, sd)
    sp[[paste0("z_", s, "_s")]] <- if (s %in% AUC_P_RATE_CATS) -z else z
  }

  z_s_cols  <- paste0("z_", scoring_cats, "_s")
  avail_s   <- z_s_cols[z_s_cols %in% names(sp)]
  wt_ps     <- wt_p[match(sub("z_(.+)_s", "\\1", avail_s), scoring_cats)]
  z_mat_ps  <- as.matrix(sp[, avail_s, drop = FALSE])
  z_mat_ps0 <- z_mat_ps; z_mat_ps0[is.na(z_mat_ps0)] <- 0
  sp$z_total_s <- as.numeric(z_mat_ps0 %*% wt_ps)
  # Additive IP volume z-score (starter-pool baseline; stored for hypothetical reuse)
  ip_mu <- NA_real_; ip_sd_val <- NA_real_
  if (ip_weight > 0) {
    ip_mu     <- mean(starters$ip, na.rm = TRUE)
    ip_sd_val <- stats::sd(starters$ip, na.rm = TRUE)
    if (is.finite(ip_sd_val) && ip_sd_val > 0) {
      z_ip_s <- z_from_ref(sp$ip, ip_mu, ip_sd_val)
      z_ip_s[is.na(z_ip_s)] <- 0
      sp$z_total_s <- sp$z_total_s + z_ip_s * ip_weight
    }
  }
  sp$z_total_s[apply(z_mat_ps, 1, function(r) all(is.na(r)))] <- NA_real_

  starters <- sp[rownames(sp) %in% rownames(starters), ]

  list(
    scores     = sp,
    starters   = starters,
    n_starters = n_start,
    params     = list(
      means         = stat_means,
      sds           = stat_sds,
      selected_cats = scoring_cats,
      is_rate       = AUC_P_RATE_CATS,
      vol_cats      = intersect(c("era", "whip"), scoring_cats),
      is_sb         = FALSE,
      cat_weights   = cat_weights,
      ip_weight     = ip_weight,
      ip_mean       = ip_mu,
      ip_sd         = ip_sd_val
    )
  )
}

compute_roto_dollars <- function(z_result, budget, min_bid = 1) {
  sp  <- z_result$starters
  all <- z_result$scores
  n   <- z_result$n_starters

  z_repl  <- min(sp$z_total_s, na.rm = TRUE)
  z_above <- sum(sp$z_total_s - z_repl, na.rm = TRUE)
  if (!is.finite(z_above) || z_above <= 0) {
    all$dollar_value <- NA_real_
    return(all)
  }
  dpz <- (budget - n * min_bid) / z_above
  all$dollar_value <- (all$z_total_s - z_repl) * dpz + min_bid
  all$dollar_value[is.na(all$z_total_s)] <- NA_real_
  all
}

# ── Points math ───────────────────────────────────────────────────────────────

compute_points_h <- function(agg_h, pts_spec) {
  total <- rep(0, nrow(agg_h))
  for (s in names(pts_spec)) {
    if (s %in% names(agg_h)) {
      vals <- suppressWarnings(as.numeric(agg_h[[s]]))
      vals[is.na(vals)] <- 0
      cat_pts <- vals * pts_spec[[s]]
      agg_h[[paste0(s, "_pts")]] <- round(cat_pts, 1)
      total <- total + cat_pts
    }
  }
  agg_h$total_pts <- round(total, 1)
  agg_h$pts_per_g <- if ("g" %in% names(agg_h) && !all(is.na(agg_h$g))) {
    round(total / pmax(agg_h$g, 1), 2)
  } else NA_real_
  agg_h
}

compute_points_p <- function(agg_p, pts_spec) {
  total <- rep(0, nrow(agg_p))
  for (s in names(pts_spec)) {
    if (s %in% names(agg_p)) {
      vals <- suppressWarnings(as.numeric(agg_p[[s]]))
      vals[is.na(vals)] <- 0
      cat_pts <- vals * pts_spec[[s]]
      agg_p[[paste0(s, "_pts")]] <- round(cat_pts, 1)
      total <- total + cat_pts
    }
  }
  agg_p$total_pts <- round(total, 1)
  agg_p$pts_per_ip <- if ("ip" %in% names(agg_p) && !all(is.na(agg_p$ip))) {
    round(total / pmax(agg_p$ip, 0.01), 2)
  } else NA_real_
  agg_p$.is_sp <- classify_sp(agg_p)
  agg_p
}

# ── Upload parsers ────────────────────────────────────────────────────────────

# Read hitters from an .xlsx file: tries "Aggregate Hitters", "Aggregate", then sheet 1
parse_agg_file_h <- function(path) {
  sheets <- tryCatch(openxlsx::getSheetNames(path), error = function(e) NULL)
  if (is.null(sheets)) return(list(data = NULL, error = "Could not read file. Is it a valid .xlsx?"))
  read_safe <- function(nm) {
    if (!nm %in% sheets) return(NULL)
    tryCatch(openxlsx::read.xlsx(path, sheet = nm, check.names = FALSE), error = function(e) NULL)
  }
  df <- read_safe("Aggregate Hitters") %||% read_safe("Aggregate") %||%
        tryCatch(openxlsx::read.xlsx(path, sheet = 1, check.names = FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0)
    return(list(data = NULL, error = "Could not find hitter data in the uploaded file."))
  names(df) <- tolower(names(df))
  list(data = df, error = NULL)
}

# Read pitchers from an .xlsx file: tries "Aggregate Pitchers", then sheet 1
parse_agg_file_p <- function(path) {
  sheets <- tryCatch(openxlsx::getSheetNames(path), error = function(e) NULL)
  if (is.null(sheets)) return(list(data = NULL, error = "Could not read file. Is it a valid .xlsx?"))
  read_safe <- function(nm) {
    if (!nm %in% sheets) return(NULL)
    tryCatch(openxlsx::read.xlsx(path, sheet = nm, check.names = FALSE), error = function(e) NULL)
  }
  df <- read_safe("Aggregate Pitchers") %||%
        tryCatch(openxlsx::read.xlsx(path, sheet = 1, check.names = FALSE), error = function(e) NULL)
  if (is.null(df) || nrow(df) == 0)
    return(list(data = NULL, error = "Could not find pitcher data in the uploaded file."))
  names(df) <- tolower(names(df))
  list(data = df, error = NULL)
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

# ── Inline weight panel (mirrors aggregator, shared helper) ───────────────────

auc_system_row_ui <- function(ns, key, label, default_wt_h, default_wt_p, suffix) {
  wt <- if (suffix == "h") default_wt_h else default_wt_p
  div(
    class = "pag-sys-row",
    div(class = "pag-sys-label",
        tags$span(class = "pag-sys-name", label)),
    div(class = "pag-sys-input",
        numericInput(
          ns(paste0("auc_wt_", suffix, "_", key)),
          label = NULL, value = wt, min = 0, step = 1, width = "70px"
        ))
  )
}

# One row in the category-weight grid: [checkbox] [label] [weight input]
auc_cat_row_ui <- function(ns, cat_key, cat_label, suffix, default_selected = TRUE, default_wt = 1) {
  div(
    class = "auc-cat-row",
    checkboxInput(
      ns(paste0("auc_cat_", suffix, "_", cat_key)),
      label = cat_label,
      value = default_selected
    ),
    numericInput(
      ns(paste0("auc_cat_wt_", suffix, "_", cat_key)),
      label = NULL, value = default_wt, min = 0, step = 0.1, width = "60px"
    )
  )
}

auc_pt_choices <- c("Weighted Average" = "aggregate",
                     setNames(PROJ_KEYS, PROJ_LABELS))

# ── UI ────────────────────────────────────────────────────────────────────────

aucValUI <- function(id) {
  ns <- NS(id)

  div(
    class = "auc-page pag-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pag-page-header",
      div(class = "pag-breadcrumb", "COLLINMYSHOT"),
      div(
        h1(class = "pag-page-title", "Auction Value Calculator"),
        p(class = "pag-page-desc",
          "Convert aggregate projections to roto z-score dollar values or Ottoneu points totals.")
      )
    ),

    # ── Scoring mode toggle bar ───────────────────────────────────────────────
    div(
      class = "auc-mode-bar",
      id    = ns("auc_mode_bar"),
      tags$button(
        id    = ns("mode_btn_roto"),
        class = "auc-mode-btn auc-mode-btn--active",
        type  = "button",
        onclick = paste0(
          "document.getElementById('", ns("mode_btn_roto"), "').classList.add('auc-mode-btn--active');",
          "document.getElementById('", ns("mode_btn_points"), "').classList.remove('auc-mode-btn--active');",
          "Shiny.setInputValue('", ns("scoring_mode"), "', 'roto', {priority:'event'});"
        ),
        tags$span(class = "auc-mode-label", "Roto"),
        tags$span(class = "auc-mode-sub",   "Z-Scores & Dollar Values")
      ),
      tags$button(
        id    = ns("mode_btn_points"),
        class = "auc-mode-btn",
        type  = "button",
        onclick = paste0(
          "document.getElementById('", ns("mode_btn_points"), "').classList.add('auc-mode-btn--active');",
          "document.getElementById('", ns("mode_btn_roto"), "').classList.remove('auc-mode-btn--active');",
          "Shiny.setInputValue('", ns("scoring_mode"), "', 'points', {priority:'event'});"
        ),
        tags$span(class = "auc-mode-label", "Points"),
        tags$span(class = "auc-mode-sub",   "Ottoneu / Custom Scoring")
      )
    ),
    # Hidden radio drives conditionalPanel JS conditions
    tags$div(style = "display:none;",
      radioButtons(ns("scoring_mode"), NULL,
                   choices = c("roto", "points"), selected = "roto")),

    # ── League Settings ───────────────────────────────────────────────────────
    div(
      class = "auc-section",
      div(class = "auc-section-title", "League Settings"),
      div(
        class = "auc-league-row",
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "# Teams"),
          tags$input(id = ns("num_teams"), type = "number",
                     value = AUC_DEFAULT_TEAMS, min = "1", step = "1",
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Budget / team ($)"),
          tags$input(id = ns("budget"), type = "text", value = AUC_DEFAULT_BUDGET,
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Starting bats / team"),
          tags$input(id = ns("start_bats"), type = "text",
                     value = AUC_DEFAULT_START_BATS,
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Starting SP / team"),
          tags$input(id = ns("start_sp"), type = "text",
                     value = AUC_DEFAULT_START_SP,
                     class = "form-control auc-budget-input")
        ),
        conditionalPanel(
          condition = "input.scoring_mode === 'roto'",
          ns = ns,
          div(
            class = "auc-league-field",
            tags$label(class = "auc-field-label", "Hitting budget %"),
            div(class = "auc-split-row",
                tags$input(id = ns("hit_pct"), type = "text",
                           value = AUC_DEFAULT_HIT_PCT,
                           class = "form-control auc-budget-input auc-split-input"),
                tags$span(class = "auc-split-sep", "/"),
                uiOutput(ns("pit_pct_display"))
            )
          )
        )
      )
    ),

    # ── Projection Source ─────────────────────────────────────────────────────
    div(
      class = "auc-section",
      div(class = "auc-section-title", "Projection Source"),
      radioButtons(
        ns("proj_source"),
        label = NULL,
        choices = c("Upload Aggregator File" = "upload",
                    "Build Inline"          = "inline"),
        selected = "inline",
        inline   = TRUE
      ),

      # Upload path
      conditionalPanel(
        condition = "input.proj_source === 'upload'",
        ns = ns,
        layout_columns(
          col_widths = c(6, 6), gap = "24px",
          div(
            div(class = "pag-col-header", "Hitters"),
            fileInput(
              ns("agg_file_h"),
              label       = NULL,
              accept      = ".xlsx",
              placeholder = "Hitter Aggregate Projection here",
              buttonLabel = "Browse"
            )
          ),
          div(
            div(class = "pag-col-header", "Pitchers"),
            fileInput(
              ns("agg_file_p"),
              label       = NULL,
              accept      = ".xlsx",
              placeholder = "Pitcher Aggregate Projection here",
              buttonLabel = "Browse"
            )
          )
        ),
        tags$p(class = "auc-upload-hint",
          "Upload .xlsx files exported from the Projection Aggregator. ",
          "Upload hitters, pitchers, or both. ",
          "For Points mode, use \u2018Build Inline\u2019 \u2014 exports may not contain all required columns (AB, 2B, 3B, HBP, CS)."
        )
      ),

      # Inline build path — Hitters (left) and Pitchers (right)
      conditionalPanel(
        condition = "input.proj_source === 'inline'",
        ns = ns,
        layout_columns(
          col_widths = c(6, 6), gap = "24px",

          # ── Hitters ──────────────────────────────────────────────────────────
          div(
            div(class = "pag-col-header", "Hitters"),
            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Projection Systems"),
              div(
                class = "pag-preset-row",
                tags$span(class = "pag-preset-label", "Weight Presets:"),
                actionButton(ns("auc_preset_h_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
                actionButton(ns("auc_preset_h_even"),   "Even",          class = "btn btn-pag-preset")
              ),
              div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude"),
              div(
                class = "auc-sys-grid",
                tagList(lapply(PROJ_SYSTEMS, function(sys) {
                  auc_system_row_ui(ns, sys$key, sys$label,
                                    PROJ_COLLIN_WT_H[sys$key],
                                    PROJ_COLLIN_WT_P[sys$key], "h")
                }))
              )
            ),
            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Playing Time"),
              div(
                class = "auc-pt-row",
                div(
                  class = "auc-pt-row-main",
                  div(
                    class = "pag-pt-source pag-pt-source--highlighted",
                    div(class = "pag-pt-source-label",
                        tags$span("\u23F1 Playing Time Source")),
                    selectInput(ns("auc_pt_h"), label = NULL,
                                choices = auc_pt_choices, selected = "atc", width = "100%"),
                    tags$p(class = "pag-pt-hint",
                           "Counting stats scaled to this system\u2019s projected PA")
                  )
                ),
                div(numericInput(ns("auc_pa_min"), "Min PA", value = 200, min = 0, step = 10, width = "90px"))
              )
            )
          ),

          # ── Pitchers ──────────────────────────────────────────────────────────
          div(
            div(class = "pag-col-header", "Pitchers"),
            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Projection Systems"),
              div(
                class = "pag-preset-row",
                tags$span(class = "pag-preset-label", "Weight Presets:"),
                actionButton(ns("auc_preset_p_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
                actionButton(ns("auc_preset_p_even"),   "Even",          class = "btn btn-pag-preset")
              ),
              div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude"),
              div(
                class = "auc-sys-grid",
                tagList(lapply(PROJ_SYSTEMS, function(sys) {
                  auc_system_row_ui(ns, sys$key, sys$label,
                                    PROJ_COLLIN_WT_H[sys$key],
                                    PROJ_COLLIN_WT_P[sys$key], "p")
                }))
              )
            ),
            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Playing Time"),
              div(
                class = "auc-pt-row",
                div(
                  class = "auc-pt-row-main",
                  div(
                    class = "pag-pt-source pag-pt-source--highlighted",
                    div(class = "pag-pt-source-label",
                        tags$span("\u23F1 Playing Time Source")),
                    selectInput(ns("auc_pt_p"), label = NULL,
                                choices = auc_pt_choices, selected = "oopsy", width = "100%"),
                    tags$p(class = "pag-pt-hint",
                           "Counting stats scaled to this system\u2019s projected IP")
                  )
                ),
                div(numericInput(ns("auc_ip_min"), "Min IP", value = 50, min = 0, step = 5, width = "90px"))
              )
            )
          )
        )  # end layout_columns
      )  # end inline conditionalPanel
    ),  # end Projection Source section

    # ── Roto Categories & Weights ─────────────────────────────────────────────
    conditionalPanel(
      condition = "input.scoring_mode === 'roto'",
      ns = ns,
      div(
        class = "auc-section",
        div(class = "auc-section-title", "Roto Categories & Weights"),
        div(
          class = "pag-preset-row",
          tags$span(class = "pag-preset-label", "Category Presets:"),
          lapply(names(AUC_ROTO_PRESETS), function(nm) {
            actionButton(ns(paste0("preset_roto_", gsub("[^a-z0-9]", "", tolower(nm)))),
                         nm, class = "btn btn-pag-preset")
          })
        ),
        layout_columns(
          col_widths = c(5, 7), gap = "20px",
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Hitting"),
            div(
              class = "pag-preset-row",
              tags$span(class = "pag-preset-label", "Weight Presets:"),
              actionButton(ns("preset_wt_ck_h"),       "CK Weights",  class = "btn btn-pag-preset btn-pag-preset--active"),
              actionButton(ns("preset_wt_equal_h"),    "Equal",       class = "btn btn-pag-preset"),
              actionButton(ns("preset_wt_novol_h"),    "No Volume",   class = "btn btn-pag-preset")
            ),
            div(
              class = "auc-vol-weight-row",
              tags$span(class = "auc-vol-weight-label", "PA Volume Weight"),
              numericInput(ns("auc_pa_wt"), label = NULL, value = 2.5, min = 0, step = 0.05, width = "70px")
            ),
            div(class = "pag-panel-subtitle", "Tick to include \u2014 weight adjusts category importance"),
            div(
              class = "auc-cat-grid",
              lapply(seq_along(AUC_H_ROTO_CATS), function(i) {
                cat <- AUC_H_ROTO_CATS[i]
                lab <- AUC_H_ROTO_LABS[i]
                if (cat %in% AUC_H_DISPLAY_ONLY) return(NULL)
                auc_cat_row_ui(ns, cat, lab, "h",
                               default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$h,
                               default_wt = AUC_H_CAT_WT_CK[[cat]] %||% 1)
              })
            )
          ),
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Pitching"),
            div(
              class = "pag-preset-row",
              tags$span(class = "pag-preset-label", "Weight Presets:"),
              actionButton(ns("preset_wt_equal_p"),  "Equal",      class = "btn btn-pag-preset btn-pag-preset--active"),
              actionButton(ns("preset_wt_novol_p"),  "No Volume",  class = "btn btn-pag-preset")
            ),
            div(
              class = "auc-vol-weight-row",
              tags$span(class = "auc-vol-weight-label", "IP Volume Weight"),
              numericInput(ns("auc_ip_wt"), label = NULL, value = 2.0, min = 0, step = 0.05, width = "70px")
            ),
            div(class = "pag-panel-subtitle",
                "Tick to include \u2014 weight adjusts importance. SVHD = SV + HD combined."),
            div(
              class = "auc-cat-grid",
              lapply(seq_along(AUC_P_ROTO_CATS), function(i) {
                cat <- AUC_P_ROTO_CATS[i]
                lab <- AUC_P_ROTO_LABS[i]
                if (cat %in% AUC_P_DISPLAY_ONLY) return(NULL)
                auc_cat_row_ui(ns, cat, lab, "p",
                               default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$p)
              })
            )
          )
        )
      )
    ),

    # ── Points Values ─────────────────────────────────────────────────────────
    conditionalPanel(
      condition = "input.scoring_mode === 'points'",
      ns = ns,
      div(
        class = "auc-section",
        div(class = "auc-section-title", "Point Values"),
        div(
          class = "auc-pts-preset-row",
          tags$span(class = "pag-preset-label", "Preset:"),
          selectInput(ns("pts_preset"), label = NULL,
                      choices = AUC_POINTS_PRESET_NAMES,
                      selected = AUC_POINTS_PRESET_NAMES[1],
                      width = "220px")
        ),
        layout_columns(
          col_widths = c(5, 7), gap = "20px",
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Hitting Point Values"),
            div(
              class = "auc-pts-grid",
              lapply(names(AUC_H_PTS_STAT_LABS), function(s) {
                div(class = "auc-pts-cell",
                    numericInput(ns(paste0("pts_h_", s)),
                                 label = AUC_H_PTS_STAT_LABS[[s]],
                                 value = AUC_POINTS_PRESETS[[1]]$h[[s]],
                                 step = 0.1, width = "80px"))
              })
            )
          ),
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Pitching Point Values"),
            div(
              class = "auc-pts-grid",
              lapply(names(AUC_P_PTS_STAT_LABS), function(s) {
                div(class = "auc-pts-cell",
                    numericInput(ns(paste0("pts_p_", s)),
                                 label = AUC_P_PTS_STAT_LABS[[s]],
                                 value = AUC_POINTS_PRESETS[[1]]$p[[s]],
                                 step = 0.1, width = "80px"))
              })
            )
          )
        )
      )
    ),

    # ── Season to Date ────────────────────────────────────────────────────────
    div(
      class = "auc-section",
      div(class = "auc-section-title", "Season to Date"),
      div(class = "auc-ytd-row",
        checkboxInput(ns("use_ytd"),
                      "Include Season-to-Date stats (auto-fetched from FanGraphs on Calculate)",
                      value = FALSE),
        tags$p(class = "auc-ytd-note",
          "Counting stats (PA, HR, R\u2026) are summed with RoS projections. ",
          "Rate stats (AVG, OBP, ERA, WHIP) are re-derived from the combined components. ",
          "Players with no matching projection appear using YTD stats only."
        )
      )
    ),

    # ── Action row ────────────────────────────────────────────────────────────
    div(
      class = "auc-action-row",
      actionButton(ns("calculate"), "Calculate Values",
                   class = "btn btn-pag-generate",
                   icon  = icon("calculator")),
      uiOutput(ns("export_btn"))
    ),

    uiOutput(ns("error_msg")),

    # ── Results ───────────────────────────────────────────────────────────────
    uiOutput(ns("results_ui"))
  )
}

# ── Draft Lab split: Projection Aggregator tab ────────────────────────────────
# Contains the projection-source config only (no scoring/valuation settings).
# Pair with aucValCalcUI() using the SAME id so they share one module instance.

aucValAggUI <- function(id, label_suffix = NULL, allow_upload = TRUE) {
  ns <- NS(id)
  sys_label <- function(k) {
    lbl <- PROJ_LABELS[[k]]
    if (!is.null(label_suffix)) paste(lbl, label_suffix) else lbl
  }
  auc_pt_choices <- c(
    "Aggregate (weighted avg)" = "aggregate",
    setNames(PROJ_KEYS, vapply(PROJ_KEYS, sys_label, character(1)))
  )

  inline_cols <- layout_columns(
    col_widths = c(6, 6), gap = "24px",

    div(
      div(class = "pag-col-header", "Hitters"),
      div(
        class = "pag-panel",
        div(class = "pag-panel-title", "Projection Systems"),
        div(
          class = "pag-preset-row",
          tags$span(class = "pag-preset-label", "Weight Presets:"),
          actionButton(ns("auc_preset_h_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
          actionButton(ns("auc_preset_h_even"),   "Even",          class = "btn btn-pag-preset")
        ),
        div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude"),
        div(
          class = "auc-sys-grid",
          tagList(lapply(PROJ_SYSTEMS, function(sys) {
            auc_system_row_ui(ns, sys$key, sys_label(sys$key),
                              PROJ_COLLIN_WT_H[sys$key],
                              PROJ_COLLIN_WT_P[sys$key], "h")
          }))
        )
      ),
      div(
        class = "pag-panel",
        div(class = "pag-panel-title", "Playing Time"),
        div(
          style = "display: flex; gap: 16px; align-items: flex-end;",
          div(
            style = "flex: 1;",
            div(
              class = "pag-pt-source pag-pt-source--highlighted",
              div(class = "pag-pt-source-label", tags$span("\u23F1 Playing Time Source")),
              selectInput(ns("auc_pt_h"), label = NULL,
                          choices = auc_pt_choices, selected = "atc", width = "100%"),
              tags$p(class = "pag-pt-hint",
                     "Counting stats scaled to this system\u2019s projected PA")
            )
          ),
          div(numericInput(ns("auc_pa_min"), "Min PA", value = 200, min = 0, step = 10, width = "90px"))
        )
      )
    ),

    div(
      div(class = "pag-col-header", "Pitchers"),
      div(
        class = "pag-panel",
        div(class = "pag-panel-title", "Projection Systems"),
        div(
          class = "pag-preset-row",
          tags$span(class = "pag-preset-label", "Weight Presets:"),
          actionButton(ns("auc_preset_p_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
          actionButton(ns("auc_preset_p_even"),   "Even",          class = "btn btn-pag-preset")
        ),
        div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude"),
        div(
          class = "auc-sys-grid",
          tagList(lapply(PROJ_SYSTEMS, function(sys) {
            auc_system_row_ui(ns, sys$key, sys_label(sys$key),
                              PROJ_COLLIN_WT_H[sys$key],
                              PROJ_COLLIN_WT_P[sys$key], "p")
          }))
        )
      ),
      div(
        class = "pag-panel",
        div(class = "pag-panel-title", "Playing Time"),
        div(
          style = "display: flex; gap: 16px; align-items: flex-end;",
          div(
            style = "flex: 1;",
            div(
              class = "pag-pt-source pag-pt-source--highlighted",
              div(class = "pag-pt-source-label", tags$span("\u23F1 Playing Time Source")),
              selectInput(ns("auc_pt_p"), label = NULL,
                          choices = auc_pt_choices, selected = "oopsy", width = "100%"),
              tags$p(class = "pag-pt-hint",
                     "Counting stats scaled to this system\u2019s projected IP")
            )
          ),
          div(numericInput(ns("auc_ip_min"), "Min IP", value = 50, min = 0, step = 5, width = "90px"))
        )
      )
    )
  )

  proj_source_section <- if (allow_upload) {
    div(
      class = "auc-section",
      div(class = "auc-section-title", "Projection Source"),
      radioButtons(
        ns("proj_source"),
        label = NULL,
        choices = c("Upload Aggregator File" = "upload",
                    "Build Inline"          = "inline"),
        selected = "inline",
        inline   = TRUE
      ),
      conditionalPanel(
        condition = "input.proj_source === 'upload'",
        ns = ns,
        layout_columns(
          col_widths = c(6, 6), gap = "24px",
          div(
            div(class = "pag-col-header", "Hitters"),
            fileInput(ns("agg_file_h"), label = NULL, accept = ".xlsx",
                      placeholder = "Hitter Aggregate Projection here",
                      buttonLabel = "Browse")
          ),
          div(
            div(class = "pag-col-header", "Pitchers"),
            fileInput(ns("agg_file_p"), label = NULL, accept = ".xlsx",
                      placeholder = "Pitcher Aggregate Projection here",
                      buttonLabel = "Browse")
          )
        ),
        tags$p(class = "auc-upload-hint",
          "Upload .xlsx files exported from the Projection Aggregator. ",
          "For Points mode use \u2018Build Inline\u2019 \u2014 exports may lack AB, 2B, 3B, HBP, CS.")
      ),
      conditionalPanel(
        condition = "input.proj_source === 'inline'",
        ns = ns,
        inline_cols
      )
    )
  } else {
    tagList(
      tags$div(style = "display:none;",
        radioButtons(ns("proj_source"), NULL,
                     choices = c("inline" = "inline"), selected = "inline")),
      inline_cols
    )
  }

  div(
    class = "auc-page pag-page",

    div(
      class = "pag-page-header",
      div(class = "pag-breadcrumb", "COLLINMYSHOT \u2014 DRAFT LAB"),
      div(
        h1(class = "pag-page-title", "Projection Aggregator"),
        p(class = "pag-page-desc",
          "Choose projection systems, set weights, and fetch raw stat projections. ",
          "Configure scoring and compute values in the Auction Value Calculator tab.")
      )
    ),

    # ── Projection Source ───────────────────────────────────────────────────
    proj_source_section,

    div(
      class = "auc-action-row",
      actionButton(ns("calculate"), "Fetch Projections",
                   class = "btn btn-pag-generate",
                   icon  = icon("download")),
      uiOutput(ns("export_btn_agg"))
    ),

    uiOutput(ns("error_msg")),

    # ── "click to fetch" message (disappears once data arrives) ────────────
    uiOutput(ns("agg_tables_ui")),

    # ── Filter checkboxes + aggregate tables ────────────────────────────────
    # Static UI: inputs are never re-created by renderUI, so user selections
    # are never silently reset by reactive re-renders of agg_h().
    navset_pill(
      nav_panel(
        "Hitters",
        div(
          class = "adp-pos-section",
          div(
            class = "adp-pos-header",
            tags$span(class = "pf-control-label", "Positions"),
            div(
              class = "adp-pos-btns",
              actionButton(ns("agg_h_pos_all"),      "Select All",   class = "btn btn-adp-pos-quick"),
              actionButton(ns("agg_h_pos_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
            )
          ),
          div(
            class = "adp-pos-checks",
            checkboxGroupInput(
              ns("pos_filter_agg_h"), label = NULL, inline = TRUE,
              choices  = c("C","1B","2B","SS","3B","OF","CI","MI","UT-only"="UT"),
              selected = c("C","1B","2B","SS","3B","OF","CI","MI","UT")
            )
          )
        ),
        spz_table_wrap(DTOutput(ns("tbl_agg_h"), width = "100%"))
      ),
      nav_panel(
        "Pitchers",
        div(
          class = "adp-pos-section",
          div(
            class = "adp-pos-header",
            tags$span(class = "pf-control-label", "Pitcher Type"),
            div(
              class = "adp-pos-btns",
              actionButton(ns("agg_p_role_all"),      "Select All",   class = "btn btn-adp-pos-quick"),
              actionButton(ns("agg_p_role_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
            )
          ),
          div(
            class = "adp-pos-checks",
            checkboxGroupInput(ns("role_filter_agg_p"), label = NULL, inline = TRUE,
                               choices = c("SP", "SP/RP", "RP"), selected = c("SP", "SP/RP", "RP"))
          )
        ),
        spz_table_wrap(DTOutput(ns("tbl_agg_p"), width = "100%"))
      )
    )
  )
}

# ── Draft Lab split: Auction Value Calculator tab ─────────────────────────────
# Contains scoring config + results only.  Pair with aucValAggUI() using the
# SAME id.  No fetch button here — results auto-render after projections are
# fetched in the Projection Aggregator tab.

aucValCalcUI <- function(id) {
  ns <- NS(id)
  div(
    class = "auc-page pag-page",

    div(
      class = "pag-page-header",
      div(class = "pag-breadcrumb", "COLLINMYSHOT \u2014 DRAFT LAB"),
      div(
        h1(class = "pag-page-title", "Auction Value Calculator"),
        p(class = "pag-page-desc",
          "Set scoring mode and category/point values, then review player rankings. ",
          "Fetch projections in the Projection Aggregator tab first.")
      )
    ),

    # Hidden step tracker
    tags$div(style = "display:none;",
      numericInput(ns("auc_wizard_step"), NULL, value = 1L, min = 1L, max = 2L, step = 1L)
    ),

    # Step indicator
    uiOutput(ns("auc_wizard_steps_ui")),

    # ── Step 1 — Scoring Rules ──────────────────────────────────────────────
    conditionalPanel(
      condition = "input.auc_wizard_step == 1", ns = ns,
      div(
        class = "auc-wizard-step",

        # ── Season to Date ────────────────────────────────────────────────────
        div(
          class = "auc-section",
          div(class = "auc-section-title", "Season to Date"),
          div(class = "auc-ytd-row",
            checkboxInput(ns("use_ytd"),
                          "Include Season-to-Date stats (auto-fetched from FanGraphs on Calculate)",
                          value = FALSE),
            tags$p(class = "auc-ytd-note",
              "Counting stats (PA, HR, R…) are summed with RoS projections. ",
              "Rate stats (AVG, OBP, ERA, WHIP) are re-derived from the combined components. ",
              "Players with no matching projection appear using YTD stats only."
            )
          )
        ),

        # Scoring mode toggle
        div(
          class = "auc-mode-bar",
          id    = ns("auc_mode_bar"),
          tags$button(
            id    = ns("mode_btn_roto"),
            class = "auc-mode-btn auc-mode-btn--active",
            type  = "button",
            onclick = paste0(
              "document.getElementById('", ns("mode_btn_roto"), "').classList.add('auc-mode-btn--active');",
              "document.getElementById('", ns("mode_btn_points"), "').classList.remove('auc-mode-btn--active');",
              "Shiny.setInputValue('", ns("scoring_mode"), "', 'roto', {priority:'event'});"
            ),
            tags$span(class = "auc-mode-label", "Roto"),
            tags$span(class = "auc-mode-sub",   "Z-Scores & Dollar Values")
          ),
          tags$button(
            id    = ns("mode_btn_points"),
            class = "auc-mode-btn",
            type  = "button",
            onclick = paste0(
              "document.getElementById('", ns("mode_btn_points"), "').classList.add('auc-mode-btn--active');",
              "document.getElementById('", ns("mode_btn_roto"), "').classList.remove('auc-mode-btn--active');",
              "Shiny.setInputValue('", ns("scoring_mode"), "', 'points', {priority:'event'});"
            ),
            tags$span(class = "auc-mode-label", "Points"),
            tags$span(class = "auc-mode-sub",   "Ottoneu / Custom Scoring")
          )
        ),
        tags$div(style = "display:none;",
          radioButtons(ns("scoring_mode"), NULL,
                       choices = c("roto", "points"), selected = "roto")),

        # League Settings
        div(
          class = "auc-section",
          div(class = "auc-section-title", "League Settings"),
          div(
            class = "auc-league-row",
            div(
              class = "auc-league-field",
              tags$label(class = "auc-field-label", "# Teams"),
              tags$input(id = ns("num_teams"), type = "number",
                         value = AUC_DEFAULT_TEAMS, min = "1", step = "1",
                         class = "form-control auc-budget-input")
            ),
            div(
              class = "auc-league-field",
              tags$label(class = "auc-field-label", "Budget / team ($)"),
              tags$input(id = ns("budget"), type = "text", value = AUC_DEFAULT_BUDGET,
                         class = "form-control auc-budget-input")
            ),
            div(
              class = "auc-league-field",
              tags$label(class = "auc-field-label", "Starting bats / team"),
              tags$input(id = ns("start_bats"), type = "text",
                         value = AUC_DEFAULT_START_BATS,
                         class = "form-control auc-budget-input")
            ),
            div(
              class = "auc-league-field",
              tags$label(class = "auc-field-label", "Starting SP / team"),
              tags$input(id = ns("start_sp"), type = "text",
                         value = AUC_DEFAULT_START_SP,
                         class = "form-control auc-budget-input")
            ),
            conditionalPanel(
              condition = "input.scoring_mode === 'roto'",
              ns = ns,
              div(
                class = "auc-league-field",
                tags$label(class = "auc-field-label", "Hitting budget %"),
                div(class = "auc-split-row",
                    tags$input(id = ns("hit_pct"), type = "text",
                               value = AUC_DEFAULT_HIT_PCT,
                               class = "form-control auc-budget-input auc-split-input"),
                    tags$span(class = "auc-split-sep", "/"),
                    uiOutput(ns("pit_pct_display"))
                )
              )
            )
          )
        ),

        # Scoring Rules — Roto
        conditionalPanel(
          condition = "input.scoring_mode === 'roto'",
          ns = ns,
          div(
            class = "auc-section",
            div(class = "auc-section-title", "Scoring Rules"),
            div(
              class = "pag-preset-row",
              tags$span(class = "pag-preset-label", "Category Presets:"),
              lapply(names(AUC_ROTO_PRESETS), function(nm) {
                actionButton(ns(paste0("preset_roto_", gsub("[^a-z0-9]", "", tolower(nm)))),
                             nm, class = "btn btn-pag-preset")
              })
            ),
            layout_columns(
              col_widths = c(5, 7), gap = "20px",
              div(
                class = "pag-panel",
                div(class = "pag-panel-title", "Hitting"),
                div(
                  class = "pag-preset-row",
                  tags$span(class = "pag-preset-label", "Weight Presets:"),
                  actionButton(ns("preset_wt_ck_h"),    "CK Weights", class = "btn btn-pag-preset btn-pag-preset--active"),
                  actionButton(ns("preset_wt_equal_h"), "Equal",      class = "btn btn-pag-preset"),
                  actionButton(ns("preset_wt_novol_h"), "No Volume",  class = "btn btn-pag-preset")
                ),
                div(
                  class = "auc-vol-weight-row",
                  tags$span(class = "auc-vol-weight-label", "PA Volume Weight"),
                  numericInput(ns("auc_pa_wt"), label = NULL, value = 2.5, min = 0, step = 0.05, width = "70px")
                ),
                div(class = "pag-panel-subtitle", "Tick to include \u2014 weight adjusts category importance"),
                div(
                  class = "auc-cat-grid",
                  lapply(seq_along(AUC_H_ROTO_CATS), function(i) {
                    cat <- AUC_H_ROTO_CATS[i]; lab <- AUC_H_ROTO_LABS[i]
                    if (cat %in% AUC_H_DISPLAY_ONLY) return(NULL)
                    auc_cat_row_ui(ns, cat, lab, "h",
                                   default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$h,
                                   default_wt = AUC_H_CAT_WT_CK[[cat]] %||% 1)
                  })
                )
              ),
              div(
                class = "pag-panel",
                div(class = "pag-panel-title", "Pitching"),
                div(
                  class = "pag-preset-row",
                  tags$span(class = "pag-preset-label", "Weight Presets:"),
                  actionButton(ns("preset_wt_equal_p"), "Equal",     class = "btn btn-pag-preset btn-pag-preset--active"),
                  actionButton(ns("preset_wt_novol_p"), "No Volume", class = "btn btn-pag-preset")
                ),
                div(
                  class = "auc-vol-weight-row",
                  tags$span(class = "auc-vol-weight-label", "IP Volume Weight"),
                  numericInput(ns("auc_ip_wt"), label = NULL, value = 2.0, min = 0, step = 0.05, width = "70px")
                ),
                div(class = "pag-panel-subtitle",
                    "Tick to include \u2014 weight adjusts importance. SVHD = SV + HD combined."),
                div(
                  class = "auc-cat-grid",
                  lapply(seq_along(AUC_P_ROTO_CATS), function(i) {
                    cat <- AUC_P_ROTO_CATS[i]; lab <- AUC_P_ROTO_LABS[i]
                    if (cat %in% AUC_P_DISPLAY_ONLY) return(NULL)
                    auc_cat_row_ui(ns, cat, lab, "p",
                                   default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$p)
                  })
                )
              )
            )
          )
        ),

        # Scoring Rules — Points
        conditionalPanel(
          condition = "input.scoring_mode === 'points'",
          ns = ns,
          div(
            class = "auc-section",
            div(class = "auc-section-title", "Scoring Rules"),
            div(
              class = "auc-pts-preset-row",
              tags$span(class = "pag-preset-label", "Preset:"),
              selectInput(ns("pts_preset"), label = NULL,
                          choices = AUC_POINTS_PRESET_NAMES,
                          selected = AUC_POINTS_PRESET_NAMES[1],
                          width = "220px")
            ),
            layout_columns(
              col_widths = c(5, 7), gap = "20px",
              div(
                class = "pag-panel",
                div(class = "pag-panel-title", "Hitter Points"),
                div(
                  class = "auc-pts-grid",
                  lapply(names(AUC_H_PTS_STAT_LABS), function(s) {
                    div(class = "auc-pts-cell",
                        numericInput(ns(paste0("pts_h_", s)),
                                     label = AUC_H_PTS_STAT_LABS[[s]],
                                     value = AUC_POINTS_PRESETS[[1]]$h[[s]],
                                     step = 0.1, width = "80px"))
                  })
                )
              ),
              div(
                class = "pag-panel",
                div(class = "pag-panel-title", "Pitcher Points"),
                div(
                  class = "auc-pts-grid",
                  lapply(names(AUC_P_PTS_STAT_LABS), function(s) {
                    div(class = "auc-pts-cell",
                        numericInput(ns(paste0("pts_p_", s)),
                                     label = AUC_P_PTS_STAT_LABS[[s]],
                                     value = AUC_POINTS_PRESETS[[1]]$p[[s]],
                                     step = 0.1, width = "80px"))
                  })
                )
              )
            )
          )
        ),

        # Navigation
        div(class = "wiz-nav",
          actionButton(ns("auc_next_1"), "Continue \u2192", class = "btn btn-primary")
        )
      )
    ),

    # ── Step 2 — Values ─────────────────────────────────────────────────────
    conditionalPanel(
      condition = "input.auc_wizard_step == 2", ns = ns,
      div(
        class = "auc-wizard-step",
        div(class = "auc-action-row", uiOutput(ns("export_btn"))),
        uiOutput(ns("results_ui")),
        div(class = "wiz-nav",
          actionButton(ns("auc_back_1"), "\u2190 Back", class = "btn btn-outline-secondary")
        )
      )
    )
  )
}

# ── Server ────────────────────────────────────────────────────────────────────

aucValServer <- function(id, adp_data = NULL, context = "standalone",
                         api_types_map = FG_API_TYPES) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── Wizard state ──────────────────────────────────────────────────────────
    rv <- reactiveValues(auc_wizard_step = 1L)

    auc_wiz_go <- function(step) {
      rv$auc_wizard_step <- step
      updateNumericInput(session, "auc_wizard_step", value = step)
    }

    output$auc_wizard_steps_ui <- renderUI({
      s <- rv$auc_wizard_step
      mk_circle <- function(n, lbl) {
        cls <- if (s > n) "wiz-circle done"
               else if (s == n) "wiz-circle active"
               else "wiz-circle"
        tags$button(
          class = "wiz-step-btn",
          onclick = sprintf("Shiny.setInputValue('%s', %d, {priority: 'event'})",
                            ns("auc_goto_step"), n),
          tags$span(class = cls, n),
          tags$span(class = "wiz-label", lbl)
        )
      }
      mk_conn <- function(n) {
        tags$div(class = if (s > n) "wiz-connector done" else "wiz-connector")
      }
      div(class = "wiz-steps",
        mk_circle(1L, "Scoring Rules"),
        mk_conn(1L),
        mk_circle(2L, "Values")
      )
    })

    observeEvent(input$auc_next_1, { auc_wiz_go(2L) })
    observeEvent(input$auc_back_1, { auc_wiz_go(1L) })
    observeEvent(input$auc_goto_step, {
      step <- as.integer(input$auc_goto_step)
      if (!is.na(step) && step >= 1L && step <= 2L) auc_wiz_go(step)
    })

    # ── Pit % display ─────────────────────────────────────────────────────────
    output$pit_pct_display <- renderUI({
      pct <- suppressWarnings(as.numeric(input$hit_pct))
      if (is.na(pct)) pct <- AUC_DEFAULT_HIT_PCT
      tags$span(class = "auc-split-pct",
                paste0(max(0, min(100, round(100 - pct))), "% pitching"))
    })

    # ── Inline weight readers ─────────────────────────────────────────────────
    read_auc_weights <- function(suffix) {
      reactive({
        setNames(
          vapply(PROJ_KEYS, function(k) {
            v <- input[[paste0("auc_wt_", suffix, "_", k)]]
            if (is.null(v) || is.na(v)) 0 else as.numeric(v)
          }, numeric(1)),
          PROJ_KEYS
        )
      })
    }
    auc_weights_h <- read_auc_weights("h")
    auc_weights_p <- read_auc_weights("p")

    # ── Preset buttons ────────────────────────────────────────────────────────
    apply_auc_preset <- function(suffix, weights) {
      for (k in PROJ_KEYS)
        updateNumericInput(session, paste0("auc_wt_", suffix, "_", k),
                           value = unname(weights[k]))
    }

    observeEvent(input$auc_preset_h_collin, {
      req(input$auc_preset_h_collin > 0)
      apply_auc_preset("h", PROJ_COLLIN_WT_H)
    })
    observeEvent(input$auc_preset_h_even, {
      req(input$auc_preset_h_even > 0)
      apply_auc_preset("h", PROJ_EVEN_WT)
    })
    observeEvent(input$auc_preset_p_collin, {
      req(input$auc_preset_p_collin > 0)
      apply_auc_preset("p", PROJ_COLLIN_WT_P)
    })
    observeEvent(input$auc_preset_p_even, {
      req(input$auc_preset_p_even > 0)
      apply_auc_preset("p", PROJ_EVEN_WT)
    })

    # ── Cat selection reactives (replace old input$roto_cats_h/p) ────────────
    roto_cats_h_sel <- reactive({
      cats <- setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY)
      sel  <- Filter(function(cat) isTRUE(input[[paste0("auc_cat_h_", cat)]]), cats)
      unique(c(AUC_H_DISPLAY_ONLY, sel))
    })
    roto_cats_p_sel <- reactive({
      cats <- setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY)
      sel  <- Filter(function(cat) isTRUE(input[[paste0("auc_cat_p_", cat)]]), cats)
      unique(c(AUC_P_DISPLAY_ONLY, sel))
    })

    # ── Cat weight reactives ──────────────────────────────────────────────────
    cat_weights_h <- reactive({
      cats <- setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY)
      setNames(vapply(cats, function(cat) {
        v <- input[[paste0("auc_cat_wt_h_", cat)]]
        if (is.null(v) || is.na(v)) 1.0 else max(0, as.numeric(v))
      }, numeric(1)), cats)
    })
    cat_weights_p <- reactive({
      cats <- setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY)
      setNames(vapply(cats, function(cat) {
        v <- input[[paste0("auc_cat_wt_p_", cat)]]
        if (is.null(v) || is.na(v)) 1.0 else max(0, as.numeric(v))
      }, numeric(1)), cats)
    })

    # ── Roto preset buttons ───────────────────────────────────────────────────
    lapply(names(AUC_ROTO_PRESETS), function(nm) {
      btn_id <- paste0("preset_roto_", gsub("[^a-z0-9]", "", tolower(nm)))
      observeEvent(input[[btn_id]], {
        req(input[[btn_id]] > 0)
        h_sel <- AUC_ROTO_PRESETS[[nm]]$h
        p_sel <- AUC_ROTO_PRESETS[[nm]]$p
        for (cat in setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY))
          updateCheckboxInput(session, paste0("auc_cat_h_", cat), value = cat %in% h_sel)
        for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
          updateCheckboxInput(session, paste0("auc_cat_p_", cat), value = cat %in% p_sel)
      })
    })

    # ── Category weight presets ───────────────────────────────────────────────
    apply_h_cat_weights <- function(wts) {
      for (cat in setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY)) {
        w <- wts[[cat]]
        updateNumericInput(session, paste0("auc_cat_wt_h_", cat), value = if (is.null(w)) 1 else w)
      }
    }
    observeEvent(input$preset_wt_ck_h, {
      req(input$preset_wt_ck_h > 0)
      apply_h_cat_weights(AUC_H_CAT_WT_CK)
      updateNumericInput(session, "auc_pa_wt", value = 2.5)
    })
    observeEvent(input$preset_wt_equal_h, {
      req(input$preset_wt_equal_h > 0)
      apply_h_cat_weights(AUC_H_CAT_WT_DEFAULT)
      updateNumericInput(session, "auc_pa_wt", value = 2.5)
    })
    observeEvent(input$preset_wt_equal_p, {
      req(input$preset_wt_equal_p > 0)
      for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_p_", cat), value = 1)
      updateNumericInput(session, "auc_ip_wt", value = 1)
    })
    observeEvent(input$preset_wt_novol_h, {
      req(input$preset_wt_novol_h > 0)
      apply_h_cat_weights(AUC_H_CAT_WT_DEFAULT)
      updateNumericInput(session, "auc_pa_wt", value = 0)
    })
    observeEvent(input$preset_wt_novol_p, {
      req(input$preset_wt_novol_p > 0)
      for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_p_", cat), value = 1)
      updateNumericInput(session, "auc_ip_wt", value = 0)
    })

    # ── SVHD exclusivity ──────────────────────────────────────────────────────
    observeEvent(input$auc_cat_p_svhd, {
      if (isTRUE(input$auc_cat_p_svhd)) {
        updateCheckboxInput(session, "auc_cat_p_sv", value = FALSE)
        updateCheckboxInput(session, "auc_cat_p_hd", value = FALSE)
      }
    }, ignoreInit = TRUE)
    observeEvent(input$auc_cat_p_sv, {
      if (isTRUE(input$auc_cat_p_sv))
        updateCheckboxInput(session, "auc_cat_p_svhd", value = FALSE)
    }, ignoreInit = TRUE)
    observeEvent(input$auc_cat_p_hd, {
      if (isTRUE(input$auc_cat_p_hd))
        updateCheckboxInput(session, "auc_cat_p_svhd", value = FALSE)
    }, ignoreInit = TRUE)

    # ── Points preset loader ──────────────────────────────────────────────────
    observeEvent(input$pts_preset, {
      nm  <- input$pts_preset
      if (!nm %in% names(AUC_POINTS_PRESETS)) return()
      spec_h <- AUC_POINTS_PRESETS[[nm]]$h
      spec_p <- AUC_POINTS_PRESETS[[nm]]$p
      for (s in names(spec_h))
        updateNumericInput(session, paste0("pts_h_", s), value = spec_h[[s]])
      for (s in names(spec_p))
        updateNumericInput(session, paste0("pts_p_", s), value = spec_p[[s]])
    })

    # ── Projection data cache ─────────────────────────────────────────────────
    fetched_h   <- reactiveVal(setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS))
    fetched_p   <- reactiveVal(setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS))
    has_data_h  <- reactiveVal(setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS))
    has_data_p  <- reactiveVal(setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS))
    calculated  <- reactiveVal(FALSE)
    last_error  <- reactiveVal(NULL)

    # ── Position eligibility cache ────────────────────────────────────────────
    # Populated when "Fetch Projections" is clicked; NULL until then.
    # build_position_eligibility() is sourced from R/position_eligibility.R.
    pos_elig_rv <- reactiveVal(NULL)

    # ── YTD stats cache (hitters + pitchers) ─────────────────────────────────
    rv_ytd_h <- reactiveVal(NULL)
    rv_ytd_p <- reactiveVal(NULL)

    # ── Aggregated projections ────────────────────────────────────────────────
    agg_h <- reactive({
      d  <- fetched_h(); hd <- has_data_h()
      if (!any(unlist(hd))) return(NULL)
      pt  <- if (is.null(input$auc_pt_h)) "aggregate" else input$auc_pt_h
      min_pa <- if (is.null(input$auc_pa_min) || is.na(input$auc_pa_min)) 0 else as.numeric(input$auc_pa_min)
      # Include extended columns needed for points scoring (ab, x2b, x3b, hbp, cs, g)
      agg <- compute_aggregate(d, hd, auc_weights_h(),
                        c(H_STAT_COLS, "g", "ab", "x2b", "x3b", "hbp", "cs"),
                        pt_col = "pa", pt_source = pt,
                        count_stats = c(H_COUNT_STATS, "ab", "x2b", "x3b", "hbp", "cs"),
                        pt_min = min_pa)
      # Derive singles (1B) for Danny League / custom points scoring
      if (!is.null(agg) && all(c("h", "x2b", "x3b", "hr") %in% names(agg))) {
        agg$x1b <- pmax(0, agg$h - agg$x2b - agg$x3b - agg$hr, na.rm = FALSE)
        agg$x1b[is.na(agg$h) | is.na(agg$x2b) | is.na(agg$x3b) | is.na(agg$hr)] <- NA_real_
      }
      if (isTRUE(input$use_ytd)) {
        ytd <- rv_ytd_h()
        if (!is.null(ytd) && nrow(ytd) > 0) agg <- auc_merge_ytd_ros_h(agg, ytd)
      }
      agg
    })

    agg_p <- reactive({
      d  <- fetched_p(); pd <- has_data_p()
      if (!any(unlist(pd))) return(NULL)
      pt  <- if (is.null(input$auc_pt_p)) "aggregate" else input$auc_pt_p
      min_ip <- if (is.null(input$auc_ip_min) || is.na(input$auc_ip_min)) 0 else as.numeric(input$auc_ip_min)
      # Include g + gs for SP classification, plus extra cols for points scoring
      agg <- compute_aggregate(d, pd, auc_weights_p(),
                        c(P_STAT_COLS, "g", "gs", "h_allow", "bb_allow", "hbp_bat", "hr_allow",
                          "er", "qs", "bs"),
                        pt_col = "ip", pt_source = pt,
                        count_stats = c(P_COUNT_STATS, "h_allow", "bb_allow", "hbp_bat", "hr_allow",
                                        "er", "qs", "bs"),
                        pt_min = min_ip)
      # Derive ER from ERA * IP / 9 if not present or all NA (most FG projections lack er)
      if (!is.null(agg)) {
        need_er <- !("er" %in% names(agg)) || all(is.na(agg$er))
        if (need_er && all(c("era", "ip") %in% names(agg))) {
          agg$er <- round(agg$era * agg$ip / 9, 1)
        }
        # Classify SP/RP based on GS/G ratio (>= 50% GS = SP)
        agg$role <- ifelse(classify_sp(agg), "SP", "RP")
      }
      if (isTRUE(input$use_ytd)) {
        ytd <- rv_ytd_p()
        if (!is.null(ytd) && nrow(ytd) > 0) agg <- auc_merge_ytd_ros_p(agg, ytd)
      }
      agg
    })

    # ── Calculate handler ─────────────────────────────────────────────────────
    observeEvent(input$calculate, {
      last_error(NULL)
      rv_ytd_h(NULL)
      rv_ytd_p(NULL)
      mode   <- input$scoring_mode
      source <- input$proj_source

      if (source == "upload") {
        h_file <- input$agg_file_h
        p_file <- input$agg_file_p
        if (is.null(h_file) && is.null(p_file)) {
          last_error("Please upload at least one file (Hitters or Pitchers)."); return()
        }
        h_list <- setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS)
        p_list <- setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS)
        hd <- setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS)
        pd <- setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS)
        errs <- character(0)
        if (!is.null(h_file)) {
          ph <- parse_agg_file_h(h_file$datapath)
          if (!is.null(ph$error)) errs <- c(errs, ph$error)
          else { h_list[[PROJ_KEYS[1]]] <- ph$data; hd[[PROJ_KEYS[1]]] <- TRUE }
        }
        if (!is.null(p_file)) {
          pp <- parse_agg_file_p(p_file$datapath)
          if (!is.null(pp$error)) errs <- c(errs, pp$error)
          else { p_list[[PROJ_KEYS[1]]] <- pp$data; pd[[PROJ_KEYS[1]]] <- TRUE }
        }
        if (length(errs) > 0) { last_error(paste(errs, collapse = "\n")); return() }
        if (!any(unlist(hd)) && !any(unlist(pd))) {
          last_error("No valid data found in uploaded files."); return()
        }
        fetched_h(h_list); has_data_h(hd)
        fetched_p(p_list); has_data_p(pd)
      } else {
        # Inline build — fetch all systems
        h_result <- setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS)
        p_result <- setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS)
        hd <- setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS)
        pd <- setNames(rep(FALSE, length(PROJ_KEYS)), PROJ_KEYS)

        withProgress(message = "Fetching projections\u2026", value = 0, {
          n    <- length(PROJ_KEYS)
          pt_h <- input$auc_pt_h %||% "aggregate"
          pt_p <- input$auc_pt_p %||% "aggregate"

          for (i in seq_along(PROJ_KEYS)) {
            k    <- PROJ_KEYS[i]
            wt_h <- suppressWarnings(as.numeric(input[[paste0("auc_wt_h_", k)]])) %||% 0
            if (is.na(wt_h)) wt_h <- 0
            incProgress(1 / (2 * n), detail = paste("Hitters \u2014", PROJ_LABELS[[k]]))
            # Skip fetch if weight is 0 and this system is not the PT source
            if (wt_h > 0 || pt_h == k) {
              h_result[[k]] <- auc_fetch_fg_projections(k, "bat", api_types_map)
              if (!is.null(h_result[[k]])) hd[[k]] <- TRUE
            }
          }
          for (i in seq_along(PROJ_KEYS)) {
            k    <- PROJ_KEYS[i]
            wt_p <- suppressWarnings(as.numeric(input[[paste0("auc_wt_p_", k)]])) %||% 0
            if (is.na(wt_p)) wt_p <- 0
            incProgress(1 / (2 * n), detail = paste("Pitchers \u2014", PROJ_LABELS[[k]]))
            if (wt_p > 0 || pt_p == k) {
              p_result[[k]] <- auc_fetch_fg_projections(k, "pit", api_types_map)
              if (!is.null(p_result[[k]])) pd[[k]] <- TRUE
            }
          }
        })

        fetched_h(h_result); has_data_h(hd)
        fetched_p(p_result); has_data_p(pd)
      }

      calculated(TRUE)

      # ── Position eligibility — refresh alongside projections ──────────────
      # ADP data (when available) is passed as the fallback source for rookies
      # and players with no prev-season MLB fielding GP.
      withProgress(message = "Loading position eligibility\u2026", value = 0.5, {
        tryCatch({
          curr_yr     <- as.integer(format(Sys.Date(), "%Y"))
          adp_for_elig <- tryCatch(
            if (!is.null(adp_data)) adp_data() else NULL,
            error = function(e) NULL
          )
          elig <- build_position_eligibility(
            prev_season    = curr_yr - 1L,
            curr_season    = curr_yr,
            prev_threshold = 20L,
            curr_threshold = 10L,
            adp_fallback   = adp_for_elig,
            output_path    = NULL,
            verbose        = FALSE
          )
          pos_elig_rv(elig)
        }, error = function(e) {
          warning(sprintf("[pos_elig] Eligibility fetch failed: %s", conditionMessage(e)))
        })
      })

      # ── YTD fetch ─────────────────────────────────────────────────────────
      if (isTRUE(input$use_ytd)) {
        withProgress(message = "Fetching Season-to-Date stats\u2026", value = 0.8, {
          tryCatch({
            incProgress(0.4, detail = "Hitters\u2026")
            h_ytd <- auc_fetch_ytd("bat")
            incProgress(0.4, detail = "Pitchers\u2026")
            p_ytd <- auc_fetch_ytd("pit")
            if (is.null(h_ytd) && is.null(p_ytd)) {
              last_error("YTD fetch returned no data. FanGraphs may be unavailable \u2014 try again in a moment.")
            } else {
              rv_ytd_h(h_ytd)
              rv_ytd_p(p_ytd)
            }
          }, error = function(e) {
            last_error(paste("YTD fetch failed:", conditionMessage(e)))
          })
        })
      }
    })

    # ── YTD checkbox observer — auto-fetch / clear when toggled post-calculate ──
    # Handles the case where projections are already loaded (via Aggregator tab
    # "Fetch Projections") and the user then checks/unchecks the YTD box.
    observeEvent(input$use_ytd, {
      if (!isTRUE(calculated())) return()  # not loaded yet; calculate handler will fetch
      if (!isTRUE(input$use_ytd)) {
        rv_ytd_h(NULL)
        rv_ytd_p(NULL)
        return()
      }
      last_error(NULL)
      withProgress(message = "Fetching Season-to-Date stats\u2026", value = 0, {
        tryCatch({
          incProgress(0.4, detail = "Hitters\u2026")
          h_ytd <- auc_fetch_ytd("bat")
          incProgress(0.4, detail = "Pitchers\u2026")
          p_ytd <- auc_fetch_ytd("pit")
          if (is.null(h_ytd) && is.null(p_ytd)) {
            last_error("YTD fetch returned no data. FanGraphs may be unavailable \u2014 try again in a moment.")
          } else {
            rv_ytd_h(h_ytd)
            rv_ytd_p(p_ytd)
          }
        }, error = function(e) {
          last_error(paste("YTD fetch failed:", conditionMessage(e)))
        })
      })
    }, ignoreInit = TRUE)

    # ── Shared z-score reactives (roto only) — computed once, reused by result + params ──
    z_result_h <- reactive({
      req(calculated(), agg_h(), input$scoring_mode == "roto")
      cats <- roto_cats_h_sel()
      if (is.null(cats) || length(cats) == 0) return(NULL)
      n_teams <- as.integer(input$num_teams)
      n_start <- suppressWarnings(as.integer(input$start_bats))
      if (is.na(n_start)) n_start <- AUC_DEFAULT_START_BATS
      pa_min  <- suppressWarnings(as.numeric(input$auc_pa_min))
      if (is.na(pa_min)) pa_min <- 200
      compute_roto_h_zscores(agg_h(), cats, pa_min,
                              starter_count = n_teams * n_start,
                              cat_weights   = cat_weights_h(),
                              pa_weight     = suppressWarnings(as.numeric(input$auc_pa_wt)) %||% 2.5)
    })

    z_result_p <- reactive({
      req(calculated(), agg_p(), input$scoring_mode == "roto")
      cats <- roto_cats_p_sel()
      if (is.null(cats) || length(cats) == 0) return(NULL)
      n_teams <- as.integer(input$num_teams)
      n_start <- suppressWarnings(as.integer(input$start_sp))
      if (is.na(n_start)) n_start <- AUC_DEFAULT_START_SP
      ip_min  <- suppressWarnings(as.numeric(input$auc_ip_min))
      if (is.na(ip_min)) ip_min <- 50
      compute_roto_p_zscores(agg_p(), cats, ip_min,
                              starter_count = n_teams * n_start,
                              cat_weights   = cat_weights_p(),
                              ip_weight     = suppressWarnings(as.numeric(input$auc_ip_wt)) %||% 2.0)
    })

    # ── Points spec reactives — exposed for Player Comparison module ───────────
    pts_spec_h <- reactive({
      req(input$scoring_mode == "points")
      setNames(
        vapply(names(AUC_H_PTS_STAT_LABS), function(s) {
          v <- input[[paste0("pts_h_", s)]]; if (is.null(v) || is.na(v)) 0 else as.numeric(v)
        }, numeric(1)),
        names(AUC_H_PTS_STAT_LABS)
      )
    })

    pts_spec_p <- reactive({
      req(input$scoring_mode == "points")
      setNames(
        vapply(names(AUC_P_PTS_STAT_LABS), function(s) {
          v <- input[[paste0("pts_p_", s)]]; if (is.null(v) || is.na(v)) 0 else as.numeric(v)
        }, numeric(1)),
        names(AUC_P_PTS_STAT_LABS)
      )
    })

    # ── Roto dollar params — exposed for hypothetical recalculation ────────────
    roto_params_h <- reactive({
      z <- z_result_h()
      if (is.null(z)) return(NULL)
      budget  <- suppressWarnings(as.numeric(input$budget))
      if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
      hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
      if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
      n_teams <- as.integer(input$num_teams)
      h_budget <- budget * hit_pct * n_teams
      z_repl  <- min(z$starters$z_total_s, na.rm = TRUE)
      z_above <- sum(z$starters$z_total_s - z_repl, na.rm = TRUE)
      dpz <- if (is.finite(z_above) && z_above > 0)
        (h_budget - z$n_starters) / z_above else NA_real_
      list(params = z$params, z_replacement = z_repl, dollars_per_z = dpz, pt_col = "pa",
           cat_weights = cat_weights_h())
    })

    roto_params_p <- reactive({
      z <- z_result_p()
      if (is.null(z)) return(NULL)
      budget  <- suppressWarnings(as.numeric(input$budget))
      if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
      hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
      if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
      n_teams <- as.integer(input$num_teams)
      pit_budget <- budget * (1 - hit_pct) * n_teams
      z_repl  <- min(z$starters$z_total_s, na.rm = TRUE)
      z_above <- sum(z$starters$z_total_s - z_repl, na.rm = TRUE)
      dpz <- if (is.finite(z_above) && z_above > 0)
        (pit_budget - z$n_starters) / z_above else NA_real_
      list(params = z$params, z_replacement = z_repl, dollars_per_z = dpz, pt_col = "ip",
           cat_weights = cat_weights_p())
    })

    # ── Computed results ──────────────────────────────────────────────────────
    result_h <- reactive({
      req(calculated(), agg_h())
      mode <- input$scoring_mode
      ah   <- agg_h()

      if (mode == "roto") {
        z_res <- z_result_h()
        if (is.null(z_res)) return(NULL)
        hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
        if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
        budget  <- suppressWarnings(as.numeric(input$budget))
        if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
        n_teams <- as.integer(input$num_teams)
        h_budget <- budget * hit_pct * n_teams
        compute_roto_dollars(z_res, h_budget)

      } else {
        compute_points_h(ah, pts_spec_h())
      }
    })

    result_p <- reactive({
      req(calculated(), agg_p())
      mode <- input$scoring_mode
      ap   <- agg_p()

      if (mode == "roto") {
        # Use z_result_p (SP-classified, IP-floored) and compute dollar values
        z_res <- tryCatch(z_result_p(), error = function(e) NULL)
        if (is.null(z_res)) {
          # Fallback when no cats selected: return SP-filtered raw data only
          ap2 <- ap[classify_sp(ap), , drop = FALSE]
          ip_min <- suppressWarnings(as.numeric(input$auc_ip_min)) %||% 50
          ap2 <- ap2[!is.na(ap2$ip) & ap2$ip >= ip_min, , drop = FALSE]
          return(if (nrow(ap2) == 0) NULL else ap2)
        }
        budget  <- suppressWarnings(as.numeric(input$budget))
        if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
        hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
        if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
        n_teams <- as.integer(input$num_teams)
        pit_budget <- budget * (1 - hit_pct) * n_teams
        compute_roto_dollars(z_res, pit_budget)

      } else {
        compute_points_p(ap, pts_spec_p())
      }
    })

    # ── Error display ─────────────────────────────────────────────────────────
    output$error_msg <- renderUI({
      err <- last_error()
      if (is.null(err)) return(NULL)
      div(class = "auc-error-box",
          tags$strong("Error: "), err)
    })

    # ── Export button ─────────────────────────────────────────────────────────
    output$export_btn <- renderUI({
      if (!calculated()) {
        tags$button("Export Values", class = "btn btn-pag-export",
                    title = "Exports as .xlsx",
                    disabled = NA, style = "opacity:0.4;cursor:not-allowed;")
      } else {
        downloadButton(ns("download"), "Export Values", class = "btn btn-pag-export",
                       title = "Exports as .xlsx")
      }
    })

    output$download <- downloadHandler(
      filename = function() sprintf("auction_values_%s_%s.xlsx", input$scoring_mode, Sys.Date()),
      content  = function(file) {
        wb <- createWorkbook()
        rh_raw <- tryCatch(result_h(), error = function(e) NULL)
        rp_raw <- tryCatch(result_p(), error = function(e) NULL)
        # Helper: join positions/ADP onto raw results and move positions after team
        with_pos <- function(df) {
          if (is.null(df)) return(df)
          df <- join_adp(df)
          if ("positions" %in% names(df) && "team" %in% names(df)) {
            rest <- setdiff(names(df), "positions")
            idx  <- which(rest == "team")
            df   <- df[, c(rest[seq_len(idx)], "positions",
                           rest[seq(idx + 1L, length(rest))]), drop = FALSE]
          }
          df
        }
        rh <- with_pos(rh_raw)
        rp <- with_pos(rp_raw)
        if (!is.null(rh)) { addWorksheet(wb, "Hitters");  writeData(wb, "Hitters",  rh) }
        if (!is.null(rp)) { addWorksheet(wb, "Pitchers"); writeData(wb, "Pitchers", rp) }
        # make_combined calls join_adp internally — pass raw results
        comb <- make_combined(rh_raw, rp_raw, input$scoring_mode)
        if (!is.null(comb)) { addWorksheet(wb, "Combined"); writeData(wb, "Combined", comb) }
        saveWorkbook(wb, file, overwrite = TRUE)
      }
    )

    # ── Aggregate export (Projection Aggregator tab) ──────────────────────────
    output$export_btn_agg <- renderUI({
      h <- tryCatch(agg_h(), error = function(e) NULL)
      p <- tryCatch(agg_p(), error = function(e) NULL)
      if (is.null(h) && is.null(p)) {
        tags$button("Export Projections", class = "btn btn-pag-export",
                    title = "Exports as .xlsx",
                    disabled = NA, style = "opacity:0.4;cursor:not-allowed;")
      } else {
        downloadButton(ns("download_agg"), "Export Projections", class = "btn btn-pag-export",
                       title = "Exports as .xlsx")
      }
    })

    output$download_agg <- downloadHandler(
      filename = function() sprintf("projections_aggregate_%s.xlsx", Sys.Date()),
      content  = function(file) {
        wb <- createWorkbook()
        h <- tryCatch(agg_h(), error = function(e) NULL)
        p <- tryCatch(agg_p(), error = function(e) NULL)
        if (!is.null(h)) {
          h_out <- h[, intersect(c("name", "team", "pa", "r", "hr", "rbi", "sb",
                                   "avg", "obp", "ab", "x2b", "x3b", "hr", "hbp",
                                   "cs", "x1b", "g"), names(h)), drop = FALSE]
          h_out <- h_out[order(h_out$pa, decreasing = TRUE, na.last = TRUE), ]
          addWorksheet(wb, "Hitters")
          writeData(wb, "Hitters", h_out)
        }
        if (!is.null(p)) {
          p_out <- p[, intersect(c("name", "team", "ip", "w", "k", "sv", "hd",
                                   "era", "whip", "h_allow", "bb_allow", "hbp_bat",
                                   "hr_allow", "er", "qs", "bs", "gs", "g"), names(p)), drop = FALSE]
          p_out <- p_out[order(p_out$ip, decreasing = TRUE, na.last = TRUE), ]
          addWorksheet(wb, "Pitchers")
          writeData(wb, "Pitchers", p_out)
        }
        saveWorkbook(wb, file, overwrite = TRUE)
      }
    )

    # ── Results UI ────────────────────────────────────────────────────────────
    output$results_ui <- renderUI({
      if (!calculated()) {
        msg <- if (context == "draftlab")
          "Go to the Projection Aggregator tab and click \u201cFetch Projections\u201d to load data."
        else
          "Configure settings and click Calculate Values to generate results."
        return(div(class = "pf-empty", tags$p(msg)))
      }
      mode <- input$scoring_mode
      navset_pill(
        id = ns("results_tab"),

        # ── Hitters ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Hitters", value = "res_h",
          div(
            class = "adp-pos-section",
            div(
              class = "adp-pos-header",
              tags$span(class = "pf-control-label", "Positions"),
              div(
                class = "adp-pos-btns",
                actionButton(ns("h_pos_all"),      "All",          class = "btn btn-adp-pos-quick"),
                actionButton(ns("h_pos_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
              )
            ),
            div(
              class = "adp-pos-checks",
              checkboxGroupInput(
                ns("pos_filter_h"), label = NULL, inline = TRUE,
                choices  = c("C","1B","2B","SS","3B","OF","CI","MI","UT-only"="UT"),
                selected = c("C","1B","2B","SS","3B","OF","CI","MI","UT")
              )
            )
          ),
          if (mode == "points") {
            navset_pill(
              id = ns("h_pts_tabs"),
              nav_panel("Expanded",   value = "h_exp",  spz_table_wrap(DTOutput(ns("tbl_h_exp"),  width = "100%"))),
              nav_panel("Simplified", value = "h_simp", spz_table_wrap(DTOutput(ns("tbl_h_simp"), width = "100%")))
            )
          } else {
            spz_table_wrap(DTOutput(ns("tbl_h"), width = "100%"))
          }
        ),

        # ── Pitchers ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Pitchers", value = "res_p",
          div(
            class = "adp-pos-section",
            div(
              class = "adp-pos-header",
              tags$span(class = "pf-control-label", "Pitcher Type"),
              div(
                class = "adp-pos-btns",
                actionButton(ns("p_role_all"),      "Select All",   class = "btn btn-adp-pos-quick"),
                actionButton(ns("p_role_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
              )
            ),
            div(
              class = "adp-pos-checks",
              checkboxGroupInput(ns("role_filter_p"), label = NULL, inline = TRUE,
                                 choices  = c("SP", "SP/RP", "RP"),
                                 selected = if (mode == "roto") c("SP", "SP/RP") else c("SP", "SP/RP", "RP"))
            )
          ),
          if (mode == "points") {
            navset_pill(
              id = ns("p_pts_tabs"),
              nav_panel("Expanded",   value = "p_exp",  spz_table_wrap(DTOutput(ns("tbl_p_exp"),  width = "100%"))),
              nav_panel("Simplified", value = "p_simp", spz_table_wrap(DTOutput(ns("tbl_p_simp"), width = "100%")))
            )
          } else {
            spz_table_wrap(DTOutput(ns("tbl_p"), width = "100%"))
          }
        ),

        # ── Combined ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Combined", value = "res_comb",
          div(
            class = "adp-pos-section",
            div(
              class = "adp-pos-header",
              tags$span(class = "pf-control-label", "Positions"),
              div(
                class = "adp-pos-btns",
                actionButton(ns("comb_pos_all"),      "All",          class = "btn btn-adp-pos-quick"),
                actionButton(ns("comb_pos_hitters"),  "Hitters",      class = "btn btn-adp-pos-quick"),
                actionButton(ns("comb_pos_pitchers"), "Pitchers",     class = "btn btn-adp-pos-quick"),
                actionButton(ns("comb_pos_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
              )
            ),
            div(
              class = "adp-pos-checks",
              checkboxGroupInput(
                ns("pos_filter_comb"), label = NULL, inline = TRUE,
                choices  = c("C","1B","2B","SS","3B","OF","CI","MI","UT-only"="UT","P"="SP"),
                selected = c("C","1B","2B","SS","3B","OF","CI","MI","UT","SP")
              )
            )
          ),
          spz_table_wrap(DTOutput(ns("tbl_combined"), width = "100%"))
        )
      )
    })

    # ── Position quick-select buttons (Hitters tab) ───────────────────────────
    AUC_H_POS   <- c("C","1B","2B","SS","3B","OF","CI","MI","UT")
    AUC_ALL_POS <- c("C","1B","2B","SS","3B","OF","CI","MI","UT","SP")

    observeEvent(input$h_pos_all, {
      updateCheckboxGroupInput(session, "pos_filter_h", selected = AUC_H_POS)
    }, ignoreInit = TRUE)
    observeEvent(input$h_pos_deselect, {
      updateCheckboxGroupInput(session, "pos_filter_h", selected = character(0))
    }, ignoreInit = TRUE)

    # ── Position quick-select buttons (Combined tab) ───────────────────────────
    observeEvent(input$comb_pos_all, {
      updateCheckboxGroupInput(session, "pos_filter_comb", selected = AUC_ALL_POS)
    }, ignoreInit = TRUE)
    observeEvent(input$comb_pos_hitters, {
      updateCheckboxGroupInput(session, "pos_filter_comb", selected = AUC_H_POS)
    }, ignoreInit = TRUE)
    observeEvent(input$comb_pos_pitchers, {
      updateCheckboxGroupInput(session, "pos_filter_comb", selected = "SP")
    }, ignoreInit = TRUE)
    observeEvent(input$comb_pos_deselect, {
      updateCheckboxGroupInput(session, "pos_filter_comb", selected = character(0))
    }, ignoreInit = TRUE)

    # ── Position filter helper ─────────────────────────────────────────────────
    # CI expands to 1B/3B; MI expands to 2B/SS; SP matched literally.
    # NULL (all boxes deselected) → show no rows.
    # Empty vector → show no rows.
    filter_by_pos <- function(df, selected) {
      if (is.null(selected)) return(df[0L, , drop = FALSE])
      if (length(selected) == 0) return(df[0L, , drop = FALSE])
      if ("ALL" %in% selected) return(df)
      if (!("positions" %in% names(df))) return(df)
      # "UT" uses exact match (only players whose entire positions string is "UT").
      # All other positions use word-boundary regex.
      ut_selected <- "UT" %in% selected
      terms <- character(0)
      for (p in setdiff(selected, "UT")) {
        if      (p == "CI") terms <- c(terms, "1B", "3B")
        else if (p == "MI") terms <- c(terms, "2B", "SS")
        else                terms <- c(terms, p)
      }
      terms    <- unique(terms)
      # Guard: paste0() with character(0) returns "\b\b" not character(0),
      # which would match every non-empty string. Only build patterns when
      # there are actual terms to search for.
      patterns <- if (length(terms) > 0) paste0("\\b", terms, "\\b") else character(0)
      mask <- vapply(df$positions, function(pos_str) {
        if (is.na(pos_str) || pos_str == "") return(FALSE)
        pos_match <- length(patterns) > 0 &&
          any(vapply(patterns, function(pat) grepl(pat, pos_str), logical(1L)))
        ut_match  <- ut_selected && identical(trimws(pos_str), "UT")
        pos_match || ut_match
      }, logical(1L))
      df[mask, , drop = FALSE]
    }

    # ── Role filter helper ─────────────────────────────────────────────────────
    # selected = c("SP","RP") means show both; NULL means show all; length-0 = none
    filter_by_role <- function(df, selected) {
      if (is.null(selected)) return(df)
      if (length(selected) == 0) return(df[0L, , drop = FALSE])
      if (!("role" %in% names(df))) return(df)
      df[df$role %in% selected, , drop = FALSE]
    }

    # Quick-select observers for role filter in Pitchers (results) tab
    observeEvent(input$p_role_all, {
      updateCheckboxGroupInput(session, "role_filter_p", selected = c("SP", "SP/RP", "RP"))
    }, ignoreInit = TRUE)
    observeEvent(input$p_role_deselect, {
      updateCheckboxGroupInput(session, "role_filter_p", selected = character(0))
    }, ignoreInit = TRUE)

    # Quick-select observers for role filter in Pitchers (aggregator) tab
    observeEvent(input$agg_p_role_all, {
      updateCheckboxGroupInput(session, "role_filter_agg_p", selected = c("SP", "SP/RP", "RP"))
    }, ignoreInit = TRUE)
    observeEvent(input$agg_p_role_deselect, {
      updateCheckboxGroupInput(session, "role_filter_agg_p", selected = character(0))
    }, ignoreInit = TRUE)

    # Quick-select observers for position filter in Hitters (aggregator) tab
    observeEvent(input$agg_h_pos_all, {
      updateCheckboxGroupInput(session, "pos_filter_agg_h",
                               selected = c("C","1B","2B","SS","3B","OF","CI","MI","UT"))
    }, ignoreInit = TRUE)
    observeEvent(input$agg_h_pos_deselect, {
      updateCheckboxGroupInput(session, "pos_filter_agg_h", selected = character(0))
    }, ignoreInit = TRUE)

    # ── DT helpers ────────────────────────────────────────────────────────────
    make_dt_opts <- spz_dt_options(col_defs = list(), extra = list(searchDelay = 200))

    # Label maps used by renderDT functions
    AUC_H_LABEL_MAP <- c(
      rank = "#",
      name = "Player", team = "Team", pa = "PA", g = "G",
      z_total_s = "Z-Sum", dollar_value = "$",
      total_pts = "Total Pts", pts_per_g = "P/G",
      setNames(AUC_H_ROTO_LABS, AUC_H_ROTO_CATS),
      setNames(paste0("Z-", AUC_H_ROTO_LABS), paste0("z_", AUC_H_ROTO_CATS, "_s")),
      setNames(unname(AUC_H_PTS_STAT_LABS), names(AUC_H_PTS_STAT_LABS)),
      setNames(paste0(unname(AUC_H_PTS_STAT_LABS), " Pts"), paste0(names(AUC_H_PTS_STAT_LABS), "_pts")),
      positions = "Pos", adp = "ADP"
    )
    AUC_P_LABEL_MAP <- c(
      rank = "#",
      name = "Player", team = "Team", positions = "Pos", adp = "ADP", ip = "IP",
      z_total_s = "Z-Sum", dollar_value = "$",
      total_pts = "Total Pts", pts_per_ip = "P/IP",
      setNames(AUC_P_ROTO_LABS, AUC_P_ROTO_CATS),
      setNames(paste0("Z-", AUC_P_ROTO_LABS), paste0("z_", AUC_P_ROTO_CATS, "_s")),
      setNames(unname(AUC_P_PTS_STAT_LABS), names(AUC_P_PTS_STAT_LABS)),
      setNames(paste0(unname(AUC_P_PTS_STAT_LABS), " Pts"), paste0(names(AUC_P_PTS_STAT_LABS), "_pts")),
      positions = "Pos", adp = "ADP"
    )

    # ── ADP + position eligibility join helper ────────────────────────────
    # Layer 1 (ADP): sets positions string + ADP number from NFBC data.
    # Layer 2 (computed eligibility): overrides positions where GP-based data
    #   is available (prev >= 20 GP or curr >= 10 GP per position).
    #   Falls back to ADP positions for unmatched players.
    # Ohtani (hitter + pitcher rows, same team) correctly maps to one ADP entry.
    join_adp <- function(df) {
      if (is.null(df) || nrow(df) == 0) return(df)

      # Layer 1: ADP (positions fallback + ADP number)
      if (!is.null(adp_data)) {
        adp <- tryCatch(adp_data(), error = function(e) NULL)
        if (!is.null(adp) && nrow(adp) > 0) {
          key_df  <- paste(player_nk(df$name),         player_nk(df$team))
          key_adp <- paste(player_nk(adp$player_name), player_nk(adp$team))
          idx <- match(key_df, key_adp)
          df$positions <- adp$positions[idx]
          df$adp       <- round(adp$adp[idx], 1)
        }
      }

      # Layer 2: computed GP-based eligibility (overrides ADP positions where matched)
      pos_elig <- pos_elig_rv()
      if (!is.null(pos_elig) && nrow(pos_elig) > 0 &&
          "eligible_positions" %in% names(pos_elig) &&
          "name_key" %in% names(pos_elig)) {
        nk_df   <- player_nk(df$name)
        eidx    <- match(nk_df, pos_elig$name_key)
        has_elig <- !is.na(eidx)
        if (any(has_elig)) {
          if (!"positions" %in% names(df)) df$positions <- NA_character_
          df$positions[has_elig] <- pos_elig$eligible_positions[eidx[has_elig]]
        }
      }

      # Display: "UT-only" → "UT" (tables show the short label; filter uses "UT" value)
      if ("positions" %in% names(df))
        df$positions <- gsub("UT-only", "UT", df$positions, fixed = TRUE)

      df
    }

    # Columns inserted after team: "positions" when eligibility OR ADP is present;
    # "adp" only when ADP feed is active.
    adp_cols <- function() {
      has_elig <- !is.null(pos_elig_rv()) && nrow(pos_elig_rv()) > 0
      adp_val  <- tryCatch(
        if (!is.null(adp_data)) adp_data() else NULL,
        error = function(e) NULL
      )
      has_adp <- !is.null(adp_val) && is.data.frame(adp_val) && nrow(adp_val) > 0
      out <- character(0)
      if (has_elig || has_adp) out <- c(out, "positions")
      if (has_adp)             out <- c(out, "adp")
      out
    }

    fmt_display_h <- function(df, mode, selected_cats = NULL) {
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- join_adp(df)
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      if (mode == "roto") {
        cats     <- if (!is.null(selected_cats) && length(selected_cats) > 0) selected_cats else AUC_H_ROTO_CATS
        raw_cols <- intersect(cats, names(df))
        z_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
        keep <- intersect(c("name", "team", adp_cols(), "pa", raw_cols, z_cols, "z_total_s", "dollar_value", "name_search"), names(df))
      } else {
        pts_pairs <- character(0)
        for (s in names(AUC_H_PTS_STAT_LABS)) {
          if (s %in% names(df))                    pts_pairs <- c(pts_pairs, s)
          pc <- paste0(s, "_pts")
          if (pc %in% names(df))                   pts_pairs <- c(pts_pairs, pc)
        }
        keep <- intersect(c("name", "team", adp_cols(), "g", pts_pairs, "total_pts", "pts_per_g", "name_search"), names(df))
      }
      df[order(if (mode == "roto") df$dollar_value else df$total_pts,
               decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
    }

    fmt_display_p <- function(df, mode, selected_cats = NULL) {
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- join_adp(df)
      # Compute role (SP/SP-RP/RP) if not already set, store in `positions` for display.
      if (!"role" %in% names(df)) df$role <- classify_role(df)
      df$positions <- df$role
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      if (mode == "roto") {
        cats     <- if (!is.null(selected_cats) && length(selected_cats) > 0) selected_cats else AUC_P_ROTO_CATS
        raw_cols <- intersect(cats, names(df))
        if ("dollar_value" %in% names(df)) {
          keep <- intersect(c("name", "team", "positions", "adp", "ip", raw_cols,
                              "z_total_s", "dollar_value", "name_search"), names(df))
          df[order(df$dollar_value, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
        } else {
          keep <- intersect(c("name", "team", "positions", "adp", "ip", raw_cols, "name_search"), names(df))
          df[order(is.na(df$adp), df$adp, -df$ip, na.last = TRUE), keep, drop = FALSE]
        }
      } else {
        pts_pairs <- character(0)
        for (s in names(AUC_P_PTS_STAT_LABS)) {
          if (s %in% names(df))                    pts_pairs <- c(pts_pairs, s)
          pc <- paste0(s, "_pts")
          if (pc %in% names(df))                   pts_pairs <- c(pts_pairs, pc)
        }
        keep <- intersect(c("name", "team", "positions", "adp", "ip", pts_pairs, "total_pts", "pts_per_ip", "name_search"), names(df))
        df[order(df$total_pts, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      }
    }

    make_combined <- function(rh, rp, mode) {
      if (is.null(rh) && is.null(rp)) return(NULL)
      # Join ADP onto the raw results before building combined view
      rh <- join_adp(rh)
      rp <- join_adp(rp)
      # Set pitcher positions to computed role (SP/SP-RP/RP) — overrides any ADP
      # positions string since ADP uses inconsistent tags for pitchers.
      if (!is.null(rp)) rp$positions <- classify_role(rp)
      has_adp <- !is.null(adp_data) && {
        adp_val <- tryCatch(adp_data(), error = function(e) NULL)
        !is.null(adp_val) && is.data.frame(adp_val) && nrow(adp_val) > 0
      }
      val_col <- if (mode == "roto") "dollar_value" else "total_pts"

      build_row <- function(d, is_p = FALSE) {
        if (is.null(d) || !(val_col %in% names(d))) return(NULL)
        cols <- list(name = d$name, team = d$team)
        if ("positions" %in% names(d))            cols$positions <- d$positions
        if (has_adp && "adp" %in% names(d))       cols$adp       <- d$adp
        if (mode == "roto") {
          cols$z_total      <- d$z_total_s
          cols$dollar_value <- d$dollar_value
        } else {
          cols$total_pts <- d$total_pts
          # Carry per-game/per-IP rate column
          rate_col <- if (is_p) "pts_per_ip" else "pts_per_g"
          if (rate_col %in% names(d)) cols$pts_rate <- d[[rate_col]]
        }
        as.data.frame(cols, stringsAsFactors = FALSE)
      }

      comb <- rbind(build_row(rh, is_p = FALSE), build_row(rp, is_p = TRUE))
      if (is.null(comb) || nrow(comb) == 0) return(comb)

      # ── Two-way player merge ─────────────────────────────────────────────────
      # Only merge rows where BOTH name AND team match — prevents false dedup of
      # same-name players on different teams (e.g. Max Muncy LAD vs Max Muncy OAK).
      # Ohtani (same name, same team) is the only legitimate merge candidate.
      dupe_key <- paste(player_nk(comb$name), comb$team, sep = "||")
      dupe_keys <- unique(dupe_key[duplicated(dupe_key)])
      if (length(dupe_keys) > 0) {
        for (key in dupe_keys) {
          idx <- which(dupe_key == key)
          if (length(idx) >= 2) {
            comb[[val_col]][idx[1]] <- sum(comb[[val_col]][idx], na.rm = TRUE)
            if ("z_total"  %in% names(comb))
              comb$z_total[idx[1]]  <- sum(comb$z_total[idx],  na.rm = TRUE)
            if ("pts_rate" %in% names(comb))
              comb$pts_rate[idx[1]] <- mean(comb$pts_rate[idx], na.rm = TRUE)
            comb <- comb[-idx[-1], , drop = FALSE]
          }
        }
      }

      comb[order(comb[[val_col]], decreasing = TRUE, na.last = TRUE), ]
    }

    auc_datatable <- function(df, hide_last = TRUE, col_labels = NULL) {
      n_cols   <- ncol(df)
      has_rank <- identical(names(df)[1], "rank")
      name_col <- if (has_rank) 1L else 0L          # 0-based index of player name
      stat_end <- n_cols - if (hide_last) 3L else 2L # last stat column (0-based)
      col_defs <- list(
        list(className = "dt-left",   targets = name_col),
        list(className = "dt-center", targets = seq(name_col + 1L, stat_end))
      )
      if (has_rank) col_defs <- c(col_defs, list(
        list(className = "dt-center auc-rank-col", targets = 0L, width = "28px")
      ))
      if (hide_last) col_defs <- c(col_defs, list(
        list(targets = n_cols - 1L, visible = FALSE, searchable = TRUE)
      ))
      datatable(df, rownames = FALSE, filter = "none", selection = "none",
                colnames = if (!is.null(col_labels)) col_labels else names(df),
                extensions = "FixedHeader",
                options = modifyList(make_dt_opts, list(columnDefs = col_defs)),
                class = "spz-dt display nowrap")
    }

    # Prepend a fixed rank column (1:N sorted by val_col desc).
    # The df is re-sorted here so rank always reflects value order even when
    # the caller hasn't pre-sorted.
    add_rank_col <- function(df, val_col) {
      if (is.null(df) || nrow(df) == 0 || !(val_col %in% names(df))) return(df)
      df  <- df[order(df[[val_col]], decreasing = TRUE, na.last = TRUE), , drop = FALSE]
      cbind(rank = seq_len(nrow(df)), df)
    }

    # Format ADP columns if present in a DT object
    fmt_adp_cols <- function(dt, df) {
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle("positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    }

    output$tbl_h <- renderDT(server = TRUE, {
      req(result_h(), input$scoring_mode == "roto")
      cats <- roto_cats_h_sel()
      df   <- fmt_display_h(result_h(), "roto", cats)
      if (is.null(df)) return(NULL)
      df <- filter_by_pos(df, input$pos_filter_h)
      df <- add_rank_col(df, "dollar_value")
      nm <- ifelse(names(df) %in% names(AUC_H_LABEL_MAP),
                   AUC_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      count_cols <- intersect(setdiff(cats, c(AUC_H_RATE_CATS, AUC_H_DISPLAY_ONLY)), names(df))
      rate_cols  <- intersect(AUC_H_RATE_CATS, names(df))
      z_s_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
      if ("pa"           %in% names(df)) dt <- DT::formatRound(dt, "pa",          digits = 0)
      if (length(count_cols) > 0)        dt <- DT::formatRound(dt, count_cols,    digits = 1)
      if (length(rate_cols)  > 0)        dt <- DT::formatRound(dt, rate_cols,     digits = 4)
      if (length(z_s_cols)   > 0)        dt <- DT::formatRound(dt, z_s_cols,      digits = 2)
      if ("z_total_s"    %in% names(df)) dt <- DT::formatRound(dt,    "z_total_s",    digits = 2)
      if ("dollar_value" %in% names(df)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    output$tbl_p <- renderDT(server = TRUE, {
      req(result_p(), input$scoring_mode == "roto")
      cats   <- roto_cats_p_sel()
      df_raw <- result_p()
      df_raw$role <- classify_role(df_raw)
      df_raw <- filter_by_role(df_raw, input$role_filter_p)
      df   <- fmt_display_p(df_raw, "roto", cats)
      if (is.null(df)) return(NULL)
      df <- add_rank_col(df, "dollar_value")
      nm <- ifelse(names(df) %in% names(AUC_P_LABEL_MAP),
                   AUC_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      count_cols <- intersect(setdiff(cats, AUC_P_RATE_CATS), names(df))
      count_cols <- intersect(count_cols, c("ip", "w", "k", "sv", "hd", "svhd"))
      z_s_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
      if (length(count_cols) > 0)        dt <- DT::formatRound(dt, count_cols, digits = 1)
      if ("era"          %in% names(df)) dt <- DT::formatRound(dt, "era",       digits = 4)
      if ("whip"         %in% names(df)) dt <- DT::formatRound(dt, "whip",      digits = 3)
      if (length(z_s_cols)   > 0)        dt <- DT::formatRound(dt, z_s_cols,    digits = 2)
      if ("z_total_s"    %in% names(df)) dt <- DT::formatRound(dt, "z_total_s", digits = 2)
      if ("dollar_value" %in% names(df)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    output$tbl_combined <- renderDT(server = TRUE, {
      rh   <- tryCatch(result_h(), error = function(e) NULL)
      rp   <- tryCatch(result_p(), error = function(e) NULL)
      comb <- make_combined(rh, rp, input$scoring_mode)
      if (is.null(comb) || nrow(comb) == 0) return(NULL)
      comb <- filter_by_pos(comb, input$pos_filter_comb)
      val_col_comb <- if (input$scoring_mode == "roto") "dollar_value" else "total_pts"
      comb <- add_rank_col(comb, val_col_comb)
      comb$name_search <- iconv(comb$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      n_cols <- ncol(comb)
      col_defs <- list(
        list(className = "dt-center auc-rank-col", targets = 0L, width = "28px"),
        list(className = "dt-left",   targets = 1L),
        list(className = "dt-center", targets = seq(2L, n_cols - 2L)),
        list(targets = n_cols - 1L, visible = FALSE, searchable = TRUE)
      )
      comb_label_map <- c(rank = "#", name = "Player", team = "Team",
                          positions = "Pos", adp = "ADP",
                          z_total = "Z-Score", dollar_value = "$", total_pts = "Total Pts",
                          pts_rate = "Pts/G·IP")
      nm_c <- ifelse(names(comb) %in% names(comb_label_map),
                     comb_label_map[names(comb)], toupper(names(comb)))
      nm_c[length(nm_c)] <- ""
      dt <- datatable(comb, rownames = FALSE, filter = "none", selection = "none",
                      colnames = nm_c,
                      options = c(make_dt_opts, list(columnDefs = col_defs)),
                      class = "pf-dt display nowrap") |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if (input$scoring_mode == "roto") {
        if ("z_total"      %in% names(comb)) dt <- DT::formatRound(dt,    "z_total",      digits = 2)
        if ("dollar_value" %in% names(comb)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      } else {
        if ("total_pts" %in% names(comb)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
        if ("pts_rate"  %in% names(comb)) dt <- DT::formatRound(dt, "pts_rate",  digits = 2)
      }
      if ("adp"       %in% names(comb)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(comb)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    # ── Points mode: hitter tables (Expanded + Simplified) ────────────────────
    output$tbl_h_exp <- renderDT(server = TRUE, {
      req(result_h())
      df <- fmt_display_h(result_h(), "points")
      if (is.null(df)) return(NULL)
      df <- filter_by_pos(df, input$pos_filter_h)
      df <- add_rank_col(df, "total_pts")
      nm <- ifelse(names(df) %in% names(AUC_H_LABEL_MAP),
                   AUC_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("g" %in% names(df)) dt <- DT::formatRound(dt, "g", digits = 1)
      raw_cols <- intersect(names(AUC_H_PTS_STAT_LABS), names(df))
      pts_cols <- intersect(paste0(names(AUC_H_PTS_STAT_LABS), "_pts"), names(df))
      if (length(raw_cols) > 0) dt <- DT::formatRound(dt, raw_cols, digits = 1)
      if (length(pts_cols) > 0) dt <- DT::formatRound(dt, pts_cols, digits = 1)
      if ("total_pts" %in% names(df)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
      if ("pts_per_g" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_g", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    output$tbl_h_simp <- renderDT(server = TRUE, {
      req(result_h())
      rh <- join_adp(result_h())
      if (is.null(rh) || nrow(rh) == 0) return(NULL)
      rh <- filter_by_pos(rh, input$pos_filter_h)
      rh <- rh[order(rh$total_pts, decreasing = TRUE, na.last = TRUE), ]
      rh$name_search <- iconv(rh$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", adp_cols(), "total_pts", "pts_per_g", "name_search"), names(rh))
      df <- rh[, keep, drop = FALSE]
      df <- add_rank_col(df, "total_pts")
      nm <- ifelse(names(df) %in% names(AUC_H_LABEL_MAP),
                   AUC_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("total_pts" %in% names(df)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
      if ("pts_per_g" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_g", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    # ── Points mode: pitcher tables (Expanded + Simplified) ───────────────────
    output$tbl_p_exp <- renderDT(server = TRUE, {
      req(result_p())
      df_raw <- result_p()
      df_raw$role <- classify_role(df_raw)
      df_raw <- filter_by_role(df_raw, input$role_filter_p)
      df <- fmt_display_p(df_raw, "points")
      if (is.null(df)) return(NULL)
      df <- add_rank_col(df, "total_pts")
      nm <- ifelse(names(df) %in% names(AUC_P_LABEL_MAP),
                   AUC_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("ip" %in% names(df)) dt <- DT::formatRound(dt, "ip", digits = 1)
      raw_cols <- intersect(names(AUC_P_PTS_STAT_LABS), names(df))
      pts_cols <- intersect(paste0(names(AUC_P_PTS_STAT_LABS), "_pts"), names(df))
      if (length(raw_cols) > 0) dt <- DT::formatRound(dt, raw_cols, digits = 1)
      if (length(pts_cols) > 0) dt <- DT::formatRound(dt, pts_cols, digits = 1)
      if ("total_pts"  %in% names(df)) dt <- DT::formatRound(dt, "total_pts",  digits = 1)
      if ("pts_per_ip" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_ip", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    output$tbl_p_simp <- renderDT(server = TRUE, {
      req(result_p())
      rp_raw <- result_p()
      rp_raw$role <- classify_role(rp_raw)
      rp_raw <- filter_by_role(rp_raw, input$role_filter_p)
      rp <- join_adp(rp_raw)
      if (is.null(rp) || nrow(rp) == 0) return(NULL)
      rp <- rp[order(rp$total_pts, decreasing = TRUE, na.last = TRUE), ]
      rp$name_search <- iconv(rp$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", adp_cols(), "total_pts", "pts_per_ip", "name_search"), names(rp))
      df <- rp[, keep, drop = FALSE]
      df <- add_rank_col(df, "total_pts")
      nm <- ifelse(names(df) %in% names(AUC_P_LABEL_MAP),
                   AUC_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- auc_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("total_pts"  %in% names(df)) dt <- DT::formatRound(dt, "total_pts",  digits = 1)
      if ("pts_per_ip" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_ip", digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    # ── Projection Aggregator raw tables (Draft Lab Tab 1) ────────────────────
    # These render as soon as agg_h / agg_p are non-NULL (no valuation needed).

    agg_label_map_h <- c(name = "Player", team = "Team", positions = "Pos", adp = "ADP",
                         pa = "PA", r = "R", hr = "HR", rbi = "RBI", sb = "SB",
                         avg = "AVG", obp = "OBP")
    agg_label_map_p <- c(name = "Player", team = "Team", positions = "Pos", adp = "ADP",
                         ip = "IP", w = "W", k = "K", sv = "SV", hd = "HD",
                         era = "ERA", whip = "WHIP")

    # Only emits the "click to fetch" message; never contains inputs.
    # Filter checkboxes and DTOutputs now live in static UI (aucValAggUI),
    # so they are never re-created by renderUI re-renders.
    output$agg_tables_ui <- renderUI({
      h_ready <- !is.null(tryCatch(agg_h(), error = function(e) NULL))
      p_ready <- !is.null(tryCatch(agg_p(), error = function(e) NULL))
      if (!h_ready && !p_ready) {
        div(class = "pf-empty",
          tags$p("Click \u201cFetch Projections\u201d above to load aggregate projections."))
      }
    })

    # Pre-joined + pre-filtered aggregator hitter data.
    # Reactive ensures filter changes reliably invalidate the table
    # without depending on DT server-side processing state.
    agg_h_tbl <- reactive({
      df <- join_adp(req(agg_h()))
      filter_by_pos(df, input$pos_filter_agg_h)
    })

    output$tbl_agg_h <- renderDT(server = FALSE, {
      df <- req(agg_h_tbl())
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", adp_cols(),
                          "pa", "r", "hr", "rbi", "sb", "avg", "obp",
                          "name_search"), names(df))
      df   <- df[order(df$pa, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      n    <- ncol(df)
      col_defs <- list(
        list(className = "dt-left",   targets = 0L),
        list(className = "dt-center", targets = seq(1L, n - 2L)),
        list(targets = n - 1L, visible = FALSE, searchable = TRUE)
      )
      nm <- ifelse(names(df) %in% names(agg_label_map_h),
                   agg_label_map_h[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- datatable(df, rownames = FALSE, filter = "none", selection = "none",
                      colnames = nm,
                      options = c(make_dt_opts, list(columnDefs = col_defs)),
                      class = "pf-dt display nowrap") |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      for (cc in intersect(c("pa", "r", "hr", "rbi", "sb"), names(df)))
        dt <- DT::formatRound(dt, cc, digits = 0)
      for (cc in intersect(c("avg", "obp"), names(df)))
        dt <- DT::formatRound(dt, cc, digits = 4)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions",
                                              color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    output$tbl_agg_p <- renderDT(server = TRUE, {
      req(agg_p())
      df <- join_adp(agg_p())
      df$role      <- classify_role(df)
      df$positions <- df$role
      df <- filter_by_role(df, input$role_filter_agg_p)
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", "positions", "adp",
                          "ip", "w", "k", "sv", "hd", "era", "whip",
                          "name_search"), names(df))
      df   <- df[order(df$ip, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      n    <- ncol(df)
      col_defs <- list(
        list(className = "dt-left",   targets = 0L),
        list(className = "dt-center", targets = seq(1L, n - 2L)),
        list(targets = n - 1L, visible = FALSE, searchable = TRUE)
      )
      nm <- ifelse(names(df) %in% names(agg_label_map_p),
                   agg_label_map_p[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- datatable(df, rownames = FALSE, filter = "none", selection = "none",
                      colnames = nm,
                      options = c(make_dt_opts, list(columnDefs = col_defs)),
                      class = "pf-dt display nowrap") |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      for (cc in intersect(c("k", "sv", "hd"), names(df)))
        dt <- DT::formatRound(dt, cc, digits = 0)
      for (cc in intersect(c("ip", "w"), names(df)))
        dt <- DT::formatRound(dt, cc, digits = 1)
      for (cc in intersect(c("era", "whip"), names(df)))
        dt <- DT::formatRound(dt, cc, digits = 2)
      if ("adp"       %in% names(df)) dt <- DT::formatRound(dt, "adp", digits = 1) |>
        DT::formatStyle("adp", fontWeight = "700", color = "var(--primary)")
      if ("positions" %in% names(df)) dt <- DT::formatStyle(dt, "positions",
                                              color = "#4a5a4f", fontSize = "0.82rem")
      dt
    })

    # Return result reactives for parent modules (Draft Lab, Player Comparison)
    list(
      result_h        = result_h,
      result_p        = result_p,
      agg_p_all       = agg_p,       # ALL pitchers (SP + RP) — used by Team Importer
      scoring_mode    = reactive(input$scoring_mode),
      roto_params_h   = roto_params_h,
      roto_params_p   = roto_params_p,
      pts_spec_h      = pts_spec_h,
      pts_spec_p      = pts_spec_p,
      selected_cats_h = roto_cats_h_sel,
      selected_cats_p = roto_cats_p_sel
    )
  })
}
