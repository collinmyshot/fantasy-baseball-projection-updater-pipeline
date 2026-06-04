suppressPackageStartupMessages({
  library(DT)
})

# ── Constants ─────────────────────────────────────────────────────────────────

SPZ_FILE_2025 <- "data/processed/2025_sp_skillz_scores_2026_plus_model.csv"

SPZ_FILES_2026 <- list(
  std     = "data/processed/2026_sp_skillz_scores_std.csv",
  l30     = "data/processed/2026_sp_skillz_scores_l30.csv",
  mar_apr = "data/processed/2026_sp_skillz_scores_mar_apr.csv",
  may     = "data/processed/2026_sp_skillz_scores_may.csv",
  jun     = "data/processed/2026_sp_skillz_scores_jun.csv",
  jul     = "data/processed/2026_sp_skillz_scores_jul.csv",
  aug     = "data/processed/2026_sp_skillz_scores_aug.csv",
  sep_oct = "data/processed/2026_sp_skillz_scores_sep_oct.csv"
)

SPZ_PERIOD_CHOICES_2025 <- c("Season to Date" = "std")

SPZ_PERIOD_CHOICES_2026 <- c(
  "Season to Date" = "std",
  "Last 30 Days"   = "l30",
  "Mar/Apr"        = "mar_apr",
  "May"            = "may",
  "Jun"            = "jun",
  "Jul"            = "jul",
  "Aug"            = "aug",
  "Sep/Oct"        = "sep_oct"
)

SPZ_DISPLAY_COLS <- c(
  "sp_skillz_rank_stabilized", "player_name", "team", "throws", "age", "gs", "ip",
  "sp_skillz_score_stabilized", "sp_skillz_reliability",
  "k_minus_bb_pct", "whiff_pct", "ball_pct",
  "stuff_plus", "pitching_plus", "gb_pct"
)

SPZ_DISPLAY_NAMES <- c(
  "RK", "Player", "Team", "Throws", "Age", "GS", "IP",
  "Score", "Rel%",
  "K-BB%", "Whiff%", "Ball%",
  "Stuff+", "Pitching+", "GB%"
)

# Colour anchors for the Score gradient (navy → pale green → burnt orange).
# Used only as documentation; actual interpolation is done per-row in JS
# so colours remain correct regardless of DataTables sort order.
# navy=#1f3556  pale-green=#eef5ec  burnt-orange=#b77343

# Glossary entries: list(term, definition, href or NULL)
SPZ_GLOSSARY <- list(
  list(
    term = "Rel%",
    def  = "Reliability score (0\u2013100). Fraction of full signal confidence based on each metric\u2019s sample vs. its stabilization point. Low Rel% = regress score toward mean.",
    href = NULL
  ),
  list(
    term = "K-BB%",
    def  = "Strikeout rate minus walk rate. The single most predictive skill metric for pitcher quality. Highest weight in SP Skillz v2.",
    href = "https://library.fangraphs.com/pitching/k-bb/"
  ),
  list(
    term = "Whiff%",
    def  = "Whiff Rate. Percentage of swings that result in a miss (whiffs / swings). Sourced from Statcast. Stronger K% predictor than SwStr% because it isolates swing decisions.",
    href = NULL
  ),
  list(
    term = "Ball%",
    def  = "Ball Rate. Percentage of pitches called balls. Higher Ball% signals poor command and correlates with elevated WHIP.",
    href = NULL
  ),
  list(
    term = "Stuff+",
    def  = "Model-based pitch quality score (100 = league avg). Measures raw pitch characteristics \u2014 velocity, movement, release \u2014 independent of outcomes.",
    href = "https://library.fangraphs.com/pitching/stuff-plus/"
  ),
  list(
    term = "Pitching+",
    def  = "Model-based pitch effectiveness score (100 = league avg). Combines stuff and location against expected outcomes.",
    href = "https://library.fangraphs.com/pitching/stuff-plus/"
  ),
  list(
    term = "GB%",
    def  = "Groundball rate. Values at or above 55% are bolded \u2014 groundball pitchers suppress HR and hard contact.",
    href = NULL
  )
)

# ── Generate Data — FanGraphs raw pitcher stats ───────────────────────────────

SPZ_GEN_FG_BASE  <- "https://www.fangraphs.com/api/leaders/major-league/data"

# Google Sheets: Stuff+ and Pitching+ for 2025 (not yet on FanGraphs)
# Tab "spring S+ March 20": col B=Name, col N=Stuff+, col Q=Pitching+
SPZ_GSHEETS_STF_URL <- paste0(
  "https://docs.google.com/spreadsheets/d/",
  "1daR9RNic3GcfDb6FLsm2OZRBS8VkqucOqHSnIS7ru5c/",
  "export?format=csv&gid=543684644"
)

spz_fetch_gsheets_stf <- function() {
  tryCatch({
    df <- read.csv(SPZ_GSHEETS_STF_URL, stringsAsFactors = FALSE,
                   check.names = FALSE, na.strings = c("", "NA"))
    data.frame(
      player_name   = trimws(as.character(df[["Name"]])),
      mlbam_id      = suppressWarnings(as.integer(df[["MLBAM id"]])),
      stuff_plus    = suppressWarnings(as.numeric(df[["Stuff+"]])),
      pitching_plus = suppressWarnings(as.numeric(df[["Pitching+"]])),
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    message("Google Sheets Stuff+ fetch failed: ", conditionMessage(e))
    NULL
  })
}

# Fetch Savant whiff data for v2 (custom leaderboard CSV endpoint — same as pitcher dashboard)
spz_fetch_savant_whiff <- function(year) {
  url <- paste0(
    "https://baseballsavant.mlb.com/leaderboard/custom",
    "?year=", year,
    "&type=pitcher&filter=&min=1",
    "&selections=", utils::URLencode("whiff_percent", reserved = TRUE),
    "&chart=false&csv=true"
  )
  tryCatch({
    tmp <- tempfile(fileext = ".csv")
    on.exit(unlink(tmp), add = TRUE)
    ok <- tryCatch({
      download.file(url, tmp, quiet = TRUE, method = "libcurl",
                    headers = c("Accept" = "text/csv, */*",
                                "User-Agent" = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"))
      TRUE
    }, error = function(e) {
      tryCatch({ download.file(url, tmp, quiet = TRUE, method = "auto"); TRUE },
               error = function(e2) FALSE)
    })
    if (!ok) return(NULL)

    raw <- read.csv(tmp, stringsAsFactors = FALSE, check.names = FALSE)
    if (nrow(raw) == 0) return(NULL)

    pid <- suppressWarnings(as.integer(raw[["player_id"]]))
    wp  <- suppressWarnings(as.numeric(raw[["whiff_percent"]]))

    data.frame(
      player_id = pid,
      whiff_pct = wp,
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    message("Savant whiff fetch failed: ", conditionMessage(e))
    NULL
  })
}

SPZ_GEN_FG_AGENT <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
# Custom stat type matching the user's reference leaderboard URL
# IDs: W(7), L(8), ERA(14), G(13), GS(55), IP(57), H(6), BB(62), HBP(122),
#      SO(42), K/9(120), BB/9(331), K/BB(121), GB%(29), LD%(31),
#      FIP(105), xFIP(110), Stuff+(386), Pitching+(387), Location+(388)
SPZ_GEN_TYPE <- "c,7,8,14,13,55,57,-1,6,62,122,42,-1,120,331,121,29,31,-1,105,110,-1,386,387,388"

spz_gen_build_url <- function(season = 2026, month = 0) {
  paste0(
    SPZ_GEN_FG_BASE,
    "?pos=all&stats=pit&lg=all&ind=0&team=0&rost=0&players=0",
    "&qual=0",
    "&type=", SPZ_GEN_TYPE,
    "&season=", season, "&season1=", season,
    "&month=", month,
    "&pageitems=2000&pagenum=1"
  )
}

spz_gen_fetch <- function(url) {
  fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
}

spz_gen_parse <- function(result) {
  if (!isTRUE(result$ok) || is.null(result$payload))
    stop("Fetch failed: ", result$error %||% "unknown error")
  p  <- result$payload
  df <- if (is.data.frame(p)) p else if (is.data.frame(p$data)) p$data else NULL
  if (is.null(df) || nrow(df) == 0) stop("No rows in API response")
  # Drop columns that are entirely NA (the -1 separator placeholders)
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}

# Compute SP Skillz v2 scores from raw FanGraphs API data + Savant whiff data.
# Uses compute_sp_skillz_v2(): ridge-derived universal weights, 6 metrics,
# no IP paradigm blending, 2-pass starter-pool z-scoring.
spz_gen_compute <- function(df,
                             num_teams       = 15,
                             sp_depth        = 10,
                             gsheets_stf     = NULL,
                             savant_whiff    = NULL,
                             ip_min          = 5,
                             # v1 params kept for signature compat (ignored in v2)
                             low_ip_weights  = NULL,
                             high_ip_weights = NULL) {
  if (is.null(df) || nrow(df) == 0) return(NULL)

  # Standardize raw API columns into the sp_skillz format
  std <- tryCatch(
    standardize_sp_skillz_input(df),
    error = function(e) { message("spz standardize error: ", conditionMessage(e)); NULL }
  )
  if (is.null(std) || nrow(std) == 0) return(NULL)

  # Apply IP minimum filter BEFORE scoring
  if (ip_min > 0 && "ip" %in% names(std)) {
    std <- std[!is.na(std$ip) & std$ip >= ip_min, , drop = FALSE]
    if (nrow(std) == 0) return(NULL)
  }

  # 2025 override: Stuff+/Pitching+ from Google Sheets (not yet on FanGraphs)
  if (!is.null(gsheets_stf) && nrow(gsheets_stf) > 0) {
    nm_lower <- tolower(trimws(std$player_name))
    gs_lower <- tolower(trimws(gsheets_stf$player_name))
    idx      <- match(nm_lower, gs_lower)
    matched  <- !is.na(idx)
    gs_stuff  <- gsheets_stf$stuff_plus[idx[matched]]
    gs_pitch  <- gsheets_stf$pitching_plus[idx[matched]]
    std$stuff_plus[matched][!is.na(gs_stuff)]    <- gs_stuff[!is.na(gs_stuff)]
    std$pitching_plus[matched][!is.na(gs_pitch)] <- gs_pitch[!is.na(gs_pitch)]
  }

  # ── v2-specific: merge Savant whiff% and compute high_gb_flag ──────────────

  # Merge Savant whiff data by player_id
  if (!is.null(savant_whiff) && nrow(savant_whiff) > 0) {
    sw <- savant_whiff[!duplicated(savant_whiff$player_id), ]
    m  <- match(std$player_id, sw$player_id)
    std$whiff_pct <- sw$whiff_pct[m]
  } else {
    std$whiff_pct <- NA_real_
  }

  # Compute high_gb_flag from GB% (stored as fraction in FG data)
  # GB% comes through standardize_sp_skillz_input as... we need to check
  # if it's already in the data. The FG API returns "GB%" which isn't in the
  # standard mapping, so we look for it in the original df.
  if (!"gb_pct" %in% names(std)) {
    # Try to pull from original FG data
    gb_col <- grep("^GB%$|^gb%$|^gb_pct$", names(df), value = TRUE, ignore.case = TRUE)
    if (length(gb_col) > 0) {
      gb_vals <- suppressWarnings(as.numeric(df[[gb_col[1]]]))
      # Match by player_id
      fg_ids <- suppressWarnings(as.integer(
        df[[grep("xMLBAMID|xmlbamid|mlbamid|playerid", names(df), value = TRUE, ignore.case = TRUE)[1]]]
      ))
      m_gb <- match(std$player_id, fg_ids)
      std$gb_pct <- gb_vals[m_gb]
    } else {
      std$gb_pct <- NA_real_
    }
  }

  std$high_gb_flag <- as.integer(
    !is.na(std$gb_pct) & std$gb_pct >= SP_SKILLZ_V2_GB_THRESHOLD
  )

  # Extract Age from raw FG API data (not in standardize function)
  age_col <- grep("^Age$|^age$", names(df), value = TRUE, ignore.case = TRUE)
  if (length(age_col) > 0) {
    fg_ids <- suppressWarnings(as.integer(
      df[[grep("xMLBAMID|xmlbamid|mlbamid|playerid", names(df), value = TRUE, ignore.case = TRUE)[1]]]
    ))
    m_age <- match(std$player_id, fg_ids)
    std$age <- suppressWarnings(as.integer(df[[age_col[1]]][m_age]))
  } else {
    std$age <- NA_integer_
  }

  # Ensure pitches column exists for reliability calc (whiff uses pitches source)
  if (!"pitches" %in% names(std) || all(is.na(std$pitches))) {
    # Estimate pitches from TBF (rough ~3.8 pitches per TBF)
    if ("tbf" %in% names(std)) {
      std$pitches[is.na(std$pitches)] <- round(std$tbf[is.na(std$pitches)] * 3.8)
    }
  }

  # Run v2 model: universal weights, 2-pass z-scoring, no IP paradigm blending
  result <- tryCatch(
    compute_sp_skillz_v2(
      skillz_data = std,
      num_teams   = num_teams,
      sp_depth    = sp_depth
    ),
    error = function(e) { message("spz v2 compute error: ", conditionMessage(e)); NULL }
  )
  if (is.null(result)) return(NULL)

  out <- result$scores
  if (nrow(out) == 0) return(NULL)
  out
}

# Format raw scored output (from spz_gen_compute) into the display table structure.
# Separating compute from format allows data consumers (e.g. Streamonator) to work
# with the raw player_name / sp_skillz_score_stabilized columns directly.
spz_gen_format <- function(out) {
  if (is.null(out) || nrow(out) == 0) return(NULL)

  # Scale raw composite score to 100-index (100 = pool avg, ±10 ≈ ±1 SD)
  raw_scores <- out$sp_skillz_score_stabilized
  s_mu <- mean(raw_scores, na.rm = TRUE)
  s_sg <- sd(raw_scores,   na.rm = TRUE)
  indexed_scores <- if (is.na(s_sg) || s_sg == 0) {
    rep(100, length(raw_scores))
  } else {
    100 + (raw_scores - s_mu) / s_sg * 10
  }

  throws_raw <- if ("throws" %in% names(out)) toupper(trimws(as.character(out$throws))) else NA_character_

  # K-BB%: stored as fraction in FG data (0.278), display as pct (27.8)
  kbb_raw <- suppressWarnings(as.numeric(out$k_minus_bb_pct))
  kbb_display <- ifelse(!is.na(kbb_raw) & abs(kbb_raw) < 1, kbb_raw * 100, kbb_raw)

  # Ball%: may already be on 0-100 scale from v2 compute
  ball_raw <- suppressWarnings(as.numeric(out$ball_pct))
  ball_display <- ifelse(!is.na(ball_raw) & ball_raw <= 1, ball_raw * 100, ball_raw)

  # GB%: display as percentage (stored as fraction 0-1 in FG data)
  gb_raw <- suppressWarnings(as.numeric(out$gb_pct))
  gb_display <- ifelse(!is.na(gb_raw) & gb_raw <= 1, gb_raw * 100, gb_raw)

  age_val <- if ("age" %in% names(out)) as.integer(out$age) else rep(NA_integer_, nrow(out))

  data.frame(
    RK          = as.integer(out$sp_skillz_rank_stabilized),
    Player      = out$player_name,
    Team        = toupper(out$team),
    Throws      = ifelse(is.na(throws_raw) | !nzchar(throws_raw), "\u2014", throws_raw),
    Age         = age_val,
    GS          = as.integer(round(out$gs)),
    IP          = round(out$ip, 1),
    Score       = round(indexed_scores, 1),
    `Rel%`      = as.integer(round(out$sp_skillz_reliability * 100)),
    `K-BB%`     = round(kbb_display, 1),
    `Whiff%`    = round(suppressWarnings(as.numeric(out$whiff_pct)), 1),
    `Ball%`     = round(ball_display, 1),
    `Stuff+`    = as.integer(round(out$stuff_plus)),
    `Pitching+` = as.integer(round(out$pitching_plus)),
    `GB%`       = round(gb_display, 1),
    check.names      = FALSE,
    stringsAsFactors = FALSE
  )
}

# ── Helpers ───────────────────────────────────────────────────────────────────

# Convert decimal IP (195.333) to traditional baseball notation (195.1)
fmt_ip <- function(x) {
  whole  <- floor(x)
  thirds <- round((x %% 1) * 3)
  ifelse(is.na(x), NA_character_,
    ifelse(thirds == 0, as.character(whole), paste0(whole, ".", thirds))
  )
}

load_spz_data <- function(year, period) {
  path <- if (year == "2025") SPZ_FILE_2025 else SPZ_FILES_2026[[period]]
  if (is.null(path) || !file.exists(path)) return(NULL)

  dat <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)

  # Filter to starter pool
  if ("sp_skillz_starter_pool_flag" %in% names(dat)) {
    dat <- dat[dat$sp_skillz_starter_pool_flag == TRUE, ]
  }

  # Add missing columns as NA
  for (col in SPZ_DISPLAY_COLS) {
    if (!col %in% names(dat)) dat[[col]] <- NA_real_
  }
  dat <- dat[, SPZ_DISPLAY_COLS]
  names(dat) <- SPZ_DISPLAY_NAMES

  # Throws: ensure character; missing/blank → em-dash
  dat[["Throws"]] <- ifelse(
    is.na(dat[["Throws"]]) | !nzchar(trimws(as.character(dat[["Throws"]]))),
    "\u2014",
    toupper(trimws(as.character(dat[["Throws"]])))
  )

  # Format numeric columns
  dat[["Age"]]        <- as.integer(dat[["Age"]])
  dat[["GS"]]        <- as.integer(dat[["GS"]])
  dat[["IP"]]        <- round(dat[["IP"]], 3)  # keep numeric for sort; JS renders as baseball notation
  raw_scores <- dat[["Score"]]
  s_mu <- mean(raw_scores, na.rm = TRUE)
  s_sg <- sd(raw_scores,   na.rm = TRUE)
  dat[["Score"]] <- round(
    if (is.na(s_sg) || s_sg == 0) rep(100, nrow(dat)) else 100 + (raw_scores - s_mu) / s_sg * 10,
    1
  )
  dat[["Rel%"]]      <- as.integer(round(dat[["Rel%"]] * 100))

  # K-BB%: stored as decimal (0.278) → display as pct (27.8)
  kbb <- suppressWarnings(as.numeric(dat[["K-BB%"]]))
  dat[["K-BB%"]] <- round(ifelse(!is.na(kbb) & abs(kbb) < 1, kbb * 100, kbb), 1)

  # Whiff%: may already be on 0-100 scale
  if ("Whiff%" %in% names(dat)) {
    dat[["Whiff%"]] <- round(suppressWarnings(as.numeric(dat[["Whiff%"]])), 1)
  }

  # Ball%: may be fraction or already pct
  ball <- suppressWarnings(as.numeric(dat[["Ball%"]]))
  dat[["Ball%"]] <- round(ifelse(!is.na(ball) & ball <= 1, ball * 100, ball), 1)

  dat[["Stuff+"]]    <- as.integer(round(dat[["Stuff+"]]))
  dat[["Pitching+"]] <- as.integer(round(dat[["Pitching+"]]))

  # GB%: ensure numeric, convert from fraction if needed
  if ("GB%" %in% names(dat)) {
    gb <- suppressWarnings(as.numeric(dat[["GB%"]]))
    dat[["GB%"]] <- round(ifelse(!is.na(gb) & gb <= 1, gb * 100, gb), 1)
  }

  dat[order(dat$RK), ]
}

apply_spz_style <- function(dt) {
  # Score bg/color are injected via createdRow JS callback in renderDT.
  dt |>
    formatStyle("Score", fontWeight = "700", textAlign = "center") |>
    formatStyle("RK",
      color      = "#8a9a8f",
      fontWeight = "400",
      fontSize   = "0.8rem",
      textAlign  = "right"
    ) |>
    formatStyle("Player", fontWeight = "650", color = "#172733") |>
    formatStyle("Team",   color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle("Throws", color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle("Age",    color = "#8a9a8f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle(c("GS", "IP", "Rel%"),
      color     = "#8a9a8f",
      fontSize  = "0.82rem",
      textAlign = "center"
    ) |>
    formatStyle(c("K-BB%", "Whiff%", "Ball%", "Stuff+", "Pitching+", "GB%"),
      color     = "#4a5a4f",
      textAlign = "center"
    )
}

render_spz_dt <- function(dat, full_scores = NULL,
                          dom_str = "lrtip") {
  if (is.null(dat) || nrow(dat) == 0) {
    return(datatable(
      data.frame(` ` = "No data available.", check.names = FALSE),
      rownames = FALSE, options = list(dom = "t", ordering = FALSE)
    ))
  }

  has_adp   <- "ADP" %in% names(dat)
  offset    <- if (has_adp) 1L else 0L
  score_col <- 7L + offset
  ip_col    <- 6L + offset
  gb_col    <- 14L + offset

  ref_sc <- if (!is.null(full_scores)) full_scores else dat[["Score"]]
  s_min  <- min(ref_sc, na.rm = TRUE)
  s_max  <- max(ref_sc, na.rm = TRUE)
  s_med  <- median(ref_sc, na.rm = TRUE)

  col_defs <- list(
    list(className = "dt-right",  targets = 0L),
    list(className = "dt-left",   targets = 1L),
    list(className = "dt-center", targets = seq.int(2L, 14L + offset)),
    list(
      targets = ip_col,
      render  = JS(
        "function(data, type, row) {",
        "  if (type !== 'display' || data === null || data === undefined) return data;",
        "  var w = Math.floor(data);",
        "  var t = Math.round((data % 1) * 3);",
        "  return t === 0 ? String(w) : w + '.' + t;",
        "}"
      )
    ),
    list(width = "36px",  targets = 0L),             # RK
    list(width = "150px", targets = 1L),             # Player
    list(width = "52px",  targets = 2L),             # Team
    list(width = "44px",  targets = 3L + offset),    # Throws
    list(width = "36px",  targets = 4L + offset),    # Age
    list(width = "44px",  targets = 5L + offset),    # GS
    list(width = "56px",  targets = 6L + offset),    # IP
    list(width = "68px",  targets = 7L + offset),    # Score
    list(width = "50px",  targets = 8L + offset),    # Rel%
    list(width = "62px",  targets = 9L + offset),    # K-BB%
    list(width = "62px",  targets = 10L + offset),   # Whiff%
    list(width = "56px",  targets = 11L + offset),   # Ball%
    list(width = "60px",  targets = 12L + offset),   # Stuff+
    list(width = "68px",  targets = 13L + offset),   # Pitching+
    list(width = "52px",  targets = 14L + offset)    # GB%
  )
  if (has_adp) col_defs <- c(col_defs, list(list(width = "56px", targets = 3L)))

  is_paged <- dom_str != "t"
  dt <- datatable(
    dat,
    rownames   = FALSE,
    filter     = "none",
    selection  = "none",
    extensions = if (is_paged) "FixedHeader" else character(0),
    options    = list(
      dom           = if (!is_paged) "t" else
                        "<'spz-ctrl-top'<'spz-ctrl-nav'<'spz-ctrl-pager'p><'spz-ctrl-sizer'l>><'spz-ctrl-info'i>>t<'spz-ctrl-bot'<'spz-ctrl-nav'<'spz-ctrl-pager'p><'spz-ctrl-sizer'l>><'spz-ctrl-info'i>>",
      pagingType    = "full_numbers",
      ordering      = TRUE,
      pageLength    = 30L,
      lengthMenu    = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
      language      = list(lengthMenu = "Page Size: _MENU_"),
      autoWidth     = FALSE,
      fixedHeader   = is_paged,
      order         = list(list(0L, "asc")),
      createdRow = JS(sprintf(
        "function(row, data, index) {
           var score = data[%d];
           if (score === null || score === undefined || score === '') return;
           var sMin = %s, sMed = %s, sMax = %s;
           var pct;
           if (sMed > sMin && score <= sMed) {
             pct = 0.5 * (score - sMin) / (sMed - sMin);
           } else if (sMax > sMed) {
             pct = 0.5 + 0.5 * (score - sMed) / (sMax - sMed);
           } else { pct = 0.5; }
           pct = Math.max(0, Math.min(1, pct));
           var c1=[31,53,86], c2=[238,245,236], c3=[183,115,67], ca, cb, t;
           if (pct <= 0.5) { t = pct*2;       ca = c1; cb = c2; }
           else            { t = (pct-0.5)*2; ca = c2; cb = c3; }
           var r=Math.round(ca[0]+(cb[0]-ca[0])*t);
           var g=Math.round(ca[1]+(cb[1]-ca[1])*t);
           var b=Math.round(ca[2]+(cb[2]-ca[2])*t);
           var lum=0.2126*(r/255)+0.7152*(g/255)+0.0722*(b/255);
           var txt = lum < 0.45 ? '#ffffff' : '#172733';
           $('td:eq(%d)', row).css({'background-color':'rgb('+r+','+g+','+b+')','color':txt});
           var gb = parseFloat(data[%d]);
           if (!isNaN(gb) && gb >= 55) {
             $('td:eq(%d)', row).css({'font-weight':'700','color':'#2a7a4b'});
           }
         }",
        score_col, s_min, s_med, s_max, score_col,
        gb_col, gb_col
      )),
      columnDefs = col_defs
    ),
    class = "spz-dt display nowrap"
  ) |>
    apply_spz_style()

  if (has_adp) {
    dt <- dt |>
      DT::formatRound("ADP", digits = 1) |>
      DT::formatStyle("ADP",
        fontWeight = "700",
        color      = "var(--primary)",
        textAlign  = "center"
      )
  }
  dt
}

spz_glossary_ui <- function() {
  make_item <- function(entry) {
    div(
      class = "spz-gloss-item",
      tags$span(
        class = "spz-gloss-term",
        if (!is.null(entry$href)) {
          tags$a(entry$term, href = entry$href, target = "_blank", class = "spz-gloss-link")
        } else {
          entry$term
        }
      ),
      tags$span(class = "spz-gloss-def", entry$def)
    )
  }

  div(
    class = "spz-glossary",
    div(class = "spz-glossary-title", "Metric Reference"),
    div(
      class = "spz-glossary-grid",
      tagList(lapply(SPZ_GLOSSARY, make_item))
    )
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

spSkillzUI <- function(id, draft_mode = FALSE) {
  ns <- NS(id)

  div(
    class = "spz-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "SP Skillz"),
      p(
        class = "pf-subtitle",
        "Pitcher evaluation via weighted metrics \u2014 K-BB%, Whiff%, Stuff+, Pitching+, Ball%, and GB profile.",
        tags$br(),
        "Weighted composite score indexed to 100 (pool avg). \u00b110 pts \u2248 \u00b11 SD. Universal weights with reliability weighting. Starter pool only."
      )
    ),

    # ── Season + period toggles + Generate Data button (standalone only) ──────
    if (!draft_mode) tagList(
      div(
        class = "pf-controls-row",
        div(
          class = "pf-control-group",
          tags$span(class = "pf-control-label", "Season"),
          div(
            class = "pf-toggle",
            radioButtons(
              ns("year"),
              label    = NULL,
              choices  = c("2025" = "2025", "2026" = "2026"),
              selected = "2026",
              inline   = TRUE
            )
          )
        ),
        div(
          class = "pf-control-group",
          actionButton(ns("gen_fetch"), "Fetch Data",
                       class = "btn btn-pag-generate", icon = icon("rotate-right")),
          div(class = "sps-status status-shell", textOutput(ns("gen_status"), inline = TRUE))
        )
      ),
      uiOutput(ns("period_ui"))
    ),

    # ── Component weights card (standalone only) ──────────────────────────────
    if (!draft_mode)
      div(
        class = "spz-weights-card",
        div(class = "spz-weights-card-title", "Skill Weights"),
        p(class = "spz-weights-card-desc",
          "v2 model: Ridge regression with LOYO-CV, universal weights (no IP paradigm blending). ",
          "6 skill metrics: K-BB%, Whiff%, Stuff+, Pitching+, Ball%, GB flag. ",
          "Reliability is per-metric using stabilization points (K-BB% at 120 TBF, Whiff% at 100 pitches, Stuff+ at 100, Pitching+ at 300)."),
        # IP minimum + preset buttons
        div(class = "spz-preset-row",
          div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:24px;",
            tags$span(class = "spz-preset-label", "IP MIN"),
            numericInput(ns("ip_min"), NULL, value = 5, min = 0, max = 200, step = 1, width = "72px")
          ),
          tags$span(class = "spz-preset-label", "PRESET"),
          actionButton(ns("preset_empirical"), "Empirical",
                       class = "btn btn-spz-preset btn-spz-preset-active"),
          actionButton(ns("preset_even"),      "Even",
                       class = "btn btn-spz-preset")
        ),
        # Transposed table: rows = paradigms, columns = metrics
        div(class = "spz-weights-table spz-weights-table-T",
          local({
            metrics <- list(
              list("SIERA",     "w_lo_siera",   -0.76, "w_hi_siera",   -1.61),
              list("xFIP",      "w_lo_xfip",    -0.42, "w_hi_xfip",    -1.34),
              list("K-BB%",     "w_lo_kbb",      1.23, "w_hi_kbb",      2.00),
              list("Contact%",  "w_lo_contact", -2.00, "w_hi_contact", -1.73),
              list("SwStr%",    "w_lo_swstr",    1.76, "w_hi_swstr",    1.84),
              list("Ball%",     "w_lo_ball",    -0.01, "w_hi_ball",    -0.58),
              list("Stuff+",    "w_lo_stf",      1.75, "w_hi_stf",      1.56),
              list("Pitching+", "w_lo_pitch",    1.06, "w_hi_pitch",    1.63)
            )
            # Header: empty label cell + metric column headers
            hdr <- div(class = "spz-wt-row spz-wt-header",
              div(class = "spz-wt-paradigm-label"),
              tagList(lapply(metrics, function(m) div(class = "spz-wt-col-label", m[[1]])))
            )
            # Low IP row
            lo_row <- div(class = "spz-wt-row",
              div(class = "spz-wt-paradigm-label",
                "Low IP", tags$br(), tags$small("\u226480 IP")),
              tagList(lapply(metrics, function(m)
                div(class = "spz-wt-input",
                  numericInput(ns(m[[2]]), NULL, value = m[[3]], step = 0.05, width = "78px"))))
            )
            # High IP row
            hi_row <- div(class = "spz-wt-row",
              div(class = "spz-wt-paradigm-label",
                "High IP", tags$br(), tags$small("\u2265100 IP")),
              tagList(lapply(metrics, function(m)
                div(class = "spz-wt-input",
                  numericInput(ns(m[[4]]), NULL, value = m[[5]], step = 0.05, width = "78px"))))
            )
            tagList(hdr, lo_row, hi_row)
          })
        )
      ),

    # ── Content: navset (standalone) or plain table (draft_mode) ─────────────
    if (!draft_mode)
      navset_pill(
        id = ns("spz_tab"),

        nav_panel(
          title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4CA"), "Rankings"),
          value = "rankings",
          div(
            class = "sps-tab-body",
            div(
              class = "pf-controls-row spz-search-row",
              div(
                class = "spz-search-wrap",
                tags$span(class = "spz-search-icon", HTML("&#x2315;")),
                textInput(ns("search"), label = NULL,
                          placeholder = "Search player or team\u2026", width = "100%")
              )
            ),
            uiOutput(ns("body_ui"))
          )
        ),

        nav_item(div(class = "dl-tab-divider")),

        nav_panel(
          title = tagList(tags$span(class = "dl-tab-icon", "\U0001F9FE"), "Compare"),
          value = "compare",
          div(
            class = "sps-tab-body",
            div(
              class = "sps-my-wrap2",
              div(
                class = "sps-my-header",
                numericInput(ns("n_compare"), "How many pitchers on your roster?",
                             value = 5, min = 1, max = 30, step = 1, width = "220px"),
                div(class = "sps-my-io",
                  actionButton(ns("clear_compare"), "Clear All",
                               class = "btn-outline-secondary")
                )
              ),
              uiOutput(ns("compare_slots_ui")),
              DTOutput(ns("compare_dt"))
            )
          )
        )
      ),

    if (draft_mode)
      tagList(
        div(
          class = "pf-controls-row spz-search-row",
          div(
            class = "spz-search-wrap",
            tags$span(class = "spz-search-icon", HTML("&#x2315;")),
            textInput(ns("search"), label = NULL,
                      placeholder = "Search player or team\u2026", width = "100%")
          )
        ),
        uiOutput(ns("body_ui"))
      ),

    # ── API diagnostic (collapsible, helps debug column mapping) ──────────────
    if (!draft_mode)
      tags$details(
        style = "margin-top:16px",
        tags$summary(class = "sps-diag-toggle", "API Response Diagnostic"),
        verbatimTextOutput(ns("spz_diag"))
      ),

    # ── Footer + glossary (standalone only) ──────────────────────────────────
    if (!draft_mode) tagList(
      div(
        class = "pf-footer",
        tags$span(
          class = "pf-footer-text",
          "SP Skillz v2: Ridge regression model \u2014 K-BB%, Whiff%, Stuff+, Pitching+, Ball%, GB flag. ",
          "Universal weights (no IP paradigm blending). Target blend: K%=1/3, WHIP=1/3, SIERA=2/9, ERA=1/9. ",
          "Reliability weighting based on metric-specific stabilization points. Whiff% from Statcast."
        )
      ),
      spz_glossary_ui()
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

spSkillzServer <- function(id, adp_data = NULL, draft_mode = FALSE, fetch_trigger = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── ADP join helper ──────────────────────────────────────────────────────
    # Inserts ADP column (after Team) when adp_data reactive is supplied.
    join_adp_spz <- function(df) {
      if (is.null(adp_data) || is.null(df) || nrow(df) == 0) return(df)
      adp <- tryCatch(adp_data(), error = function(e) NULL)
      if (is.null(adp) || nrow(adp) == 0) return(df)
      nm_df  <- tolower(trimws(df[["Player"]]))
      nm_adp <- tolower(trimws(adp$player_name))
      idx    <- match(nm_df, nm_adp)
      adp_vec <- round(adp$adp[idx], 1)
      # Insert after Team (column 3, 1-indexed)
      cbind(df[, 1:3, drop = FALSE],
            ADP = adp_vec,
            df[, 4:ncol(df), drop = FALSE],
            stringsAsFactors = FALSE)
    }

    # Period row — only rendered in standalone mode; draft_mode locks to 2025/std
    output$period_ui <- renderUI({
      if (draft_mode) return(NULL)
      req(input$year)
      choices <- if (input$year == "2025") SPZ_PERIOD_CHOICES_2025 else SPZ_PERIOD_CHOICES_2026
      div(
        class = "pf-controls-row",
        div(
          class = "pf-control-group",
          tags$span(class = "pf-control-label", "Period"),
          div(
            class = "pf-toggle spz-period-toggle",
            radioButtons(
              ns("period"),
              label    = NULL,
              choices  = choices,
              selected = "std",
              inline   = TRUE
            )
          )
        )
      )
    })

    # FanGraphs month codes for each period key
    SPZ_MONTH_CODES <- c(std = 0L, l30 = 3L, mar_apr = 4L, may = 5L,
                         jun = 6L, jul = 7L, aug = 8L, sep_oct = 9L)

    # Live-generated data (all periods fetched on button click)
    rv_spz <- reactiveValues(
      raw_std     = NULL,   # parsed API data (not yet scored)
      raw_l30     = NULL,
      raw_mar_apr = NULL,
      raw_may     = NULL,
      raw_jun     = NULL,
      raw_jul     = NULL,
      raw_aug     = NULL,
      raw_sep_oct = NULL,
      gsheets_stf = NULL,   # Stuff+/Pitching+ from Google Sheets (2025 only)
      savant_whiff = NULL,  # Savant whiff data for v2 model
      std         = NULL,   # scored + formatted tables
      l30         = NULL,
      mar_apr     = NULL,
      may         = NULL,
      jun         = NULL,
      jul         = NULL,
      aug         = NULL,
      sep_oct     = NULL,
      status      = "Select year and click \u2018Generate Data\u2019.",
      diag        = NULL,
      fetch_state = "none"   # "none" | "ok" | "error"
    )
    output$gen_status <- renderText({ rv_spz$status })
    output$spz_diag   <- renderText({ rv_spz$diag %||% "" })

    # Helper: current weight values from inputs (falls back to defaults)
    # Return both IP paradigm weight vectors, reading from inputs with defaults
    cur_weights <- function() {
      g <- function(id, default) { v <- suppressWarnings(as.numeric(input[[id]])); if (is.na(v)) default else v }
      lo <- c(
        tbf            = 0,
        ip_per_gs      = 0,
        siera          = g("w_lo_siera",   -0.76),
        xfip           = g("w_lo_xfip",    -0.42),
        k_minus_bb_pct = g("w_lo_kbb",      1.23),
        contact_pct    = g("w_lo_contact", -2.00),
        swstr_pct      = g("w_lo_swstr",     1.76),
        ball_pct       = g("w_lo_ball",    -0.01),
        stuff_plus     = g("w_lo_stf",      1.75),
        pitching_plus  = g("w_lo_pitch",    1.06)
      )
      hi <- c(
        tbf            = 0,
        ip_per_gs      = 0,
        siera          = g("w_hi_siera",   -1.61),
        xfip           = g("w_hi_xfip",    -1.34),
        k_minus_bb_pct = g("w_hi_kbb",      2.00),
        contact_pct    = g("w_hi_contact", -1.73),
        swstr_pct      = g("w_hi_swstr",     1.84),
        ball_pct       = g("w_hi_ball",    -0.58),
        stuff_plus     = g("w_hi_stf",      1.60),
        pitching_plus  = g("w_hi_pitch",    1.63)
      )
      list(low_ip_weights = lo, high_ip_weights = hi)
    }

    # Re-score whenever raw data, weights, or IP minimum change (no re-fetch needed)
    observe({
      w      <- cur_weights()
      gs     <- rv_spz$gsheets_stf
      sw     <- rv_spz$savant_whiff
      ip_min <- as.integer(input$ip_min %||% 5)
      score_one <- function(raw) {
        spz_gen_compute(raw,
          gsheets_stf     = gs,
          savant_whiff    = sw,
          ip_min          = ip_min)
      }
      rv_spz$std     <- score_one(rv_spz$raw_std)
      rv_spz$l30     <- score_one(rv_spz$raw_l30)
      rv_spz$mar_apr <- score_one(rv_spz$raw_mar_apr)
      rv_spz$may     <- score_one(rv_spz$raw_may)
      rv_spz$jun     <- score_one(rv_spz$raw_jun)
      rv_spz$jul     <- score_one(rv_spz$raw_jul)
      rv_spz$aug     <- score_one(rv_spz$raw_aug)
      rv_spz$sep_oct <- score_one(rv_spz$raw_sep_oct)
    })

    observeEvent(input$gen_fetch, {
      yr      <- as.integer(input$year %||% "2026")
      periods <- if (yr == 2025L) c("std") else names(SPZ_MONTH_CODES)
      withProgress(message = "Fetching SP Skillz\u2026", value = 0, {
        rv_spz$status      <- "Fetching\u2026"
        rv_spz$fetch_state <- "none"
        rv_spz$gsheets_stf <- NULL
        rv_spz$savant_whiff <- NULL
        rv_spz$diag        <- NULL
        # Clear all raw slots
        for (k in names(SPZ_MONTH_CODES))
          rv_spz[[paste0("raw_", k)]] <- NULL
        # For 2025: Stuff+/Pitching+ come from Google Sheets (not yet on FanGraphs)
        if (yr == 2025L) rv_spz$gsheets_stf <- spz_fetch_gsheets_stf()
        # Fetch Savant whiff data for v2 model
        incProgress(0.05, detail = "Savant whiff data\u2026")
        rv_spz$savant_whiff <- spz_fetch_savant_whiff(yr)
        fetch_raw <- function(month) {
          tryCatch({
            url <- spz_gen_build_url(season = yr, month = month)
            res <- spz_gen_fetch(url)
            spz_gen_parse(res)
          }, error = function(e) {
            message("spz fetch error (month=", month, "): ", conditionMessage(e))
            NULL
          })
        }
        for (k in periods) {
          lbl <- names(SPZ_PERIOD_CHOICES_2026)[SPZ_PERIOD_CHOICES_2026 == k]
          incProgress(1 / length(periods),
                      detail = if (length(lbl) > 0) lbl[1] else k)
          rv_spz[[paste0("raw_", k)]] <- fetch_raw(SPZ_MONTH_CODES[[k]])
        }
        raw_std <- rv_spz$raw_std
        if (!is.null(raw_std) && nrow(raw_std) > 0) {
          n <- nrow(raw_std)
          rv_spz$status      <- sprintf("%d pitchers \u2014 %s", n, format(Sys.time(), "%I:%M %p"))
          rv_spz$fetch_state <- "ok"
          rv_spz$diag <- paste0(
            "Shape: ", nrow(raw_std), " rows \u00d7 ", ncol(raw_std), " cols\n",
            "Columns: ", paste(names(raw_std), collapse = ", "), "\n",
            "Row 1: ", paste(
              mapply(function(nm, v) paste0(nm, "=", substr(as.character(v), 1, 40)),
                     names(raw_std), as.list(raw_std[1, ])),
              collapse = " | ")
          )
        } else {
          rv_spz$status      <- paste0("Fetch failed \u2014 ", format(Sys.time(), "%I:%M %p"))
          rv_spz$fetch_state <- "error"
        }
      })
    })

    # ── Weight presets ────────────────────────────────────────────────────────
    SPZ_EMPIRICAL_WEIGHTS <- list(
      w_lo_siera = -0.76, w_lo_xfip = -0.42, w_lo_kbb =  1.23, w_lo_contact = -2.00,
      w_lo_swstr =  1.76, w_lo_ball = -0.01, w_lo_stf =  1.75, w_lo_pitch   =  1.06,
      w_hi_siera = -1.61, w_hi_xfip = -1.34, w_hi_kbb =  2.00, w_hi_contact = -1.73,
      w_hi_swstr =  1.84, w_hi_ball = -0.58, w_hi_stf =  1.60, w_hi_pitch   =  1.63
    )
    apply_weights <- function(wts) {
      for (id in names(wts)) updateNumericInput(session, id, value = wts[[id]])
    }

    observeEvent(input$preset_empirical, { apply_weights(SPZ_EMPIRICAL_WEIGHTS) })
    observeEvent(input$preset_even, {
      even <- setNames(as.list(rep(1, 16)),
        c("w_lo_siera","w_lo_xfip","w_lo_kbb","w_lo_contact",
          "w_lo_swstr","w_lo_ball","w_lo_stf","w_lo_pitch",
          "w_hi_siera","w_hi_xfip","w_hi_kbb","w_hi_contact",
          "w_hi_swstr","w_hi_ball","w_hi_stf","w_hi_pitch"))
      apply_weights(even)
    })

    spz_data <- reactive({
      if (draft_mode) return(load_spz_data("2025", "std"))
      period <- input$period %||% "std"
      yr     <- input$year   %||% "2026"
      # For CSV-backed data (file-based periods), try loading from disk
      if (yr == "2025") return(load_spz_data("2025", "std"))
      # For live-generated data: rv_spz slots hold raw scored format;
      # format for display before rendering.
      spz_gen_format(rv_spz[[period]])
    })

    # Join ADP when available (adds ADP column after Team)
    spz_data_adp <- reactive({
      join_adp_spz(spz_data())
    })

    search_d <- debounce(reactive(input$search), 300)

    spz_filtered <- reactive({
      dat <- spz_data_adp()
      if (is.null(dat) || nrow(dat) == 0) return(NULL)
      q   <- trimws(search_d() %||% "")
      if (nchar(q) == 0) return(dat)
      mask <- grepl(q, dat[["Player"]], ignore.case = TRUE) |
              grepl(q, dat[["Team"]],   ignore.case = TRUE)
      dat[mask, ]
    })

    output$body_ui <- renderUI({
      dat   <- spz_data_adp()
      state <- rv_spz$fetch_state
      yr    <- input$year %||% "2026"
      if (!is.null(dat) && nrow(dat) > 0) {
        div(class = "spz-table-wrap", DTOutput(ns("table"), width = "100%"))
      } else if (state == "error") {
        div(
          class = "spz-empty",
          div(
            class = "spz-empty-inner",
            h3(class = "spz-empty-title", "\u26a0\ufe0f Fetch failed"),
            p(class = "spz-empty-desc",
              "Could not retrieve data from FanGraphs. This may be a temporary issue.",
              tags$br(),
              "Please wait a moment and click \u2018Generate Data\u2019 again."
            )
          )
        )
      } else {
        div(
          class = "spz-empty",
          div(
            class = "spz-empty-inner",
            h3(class = "spz-empty-title", "No data loaded"),
            p(class = "spz-empty-desc",
              if (yr == "2025")
                "No 2025 data file found."
              else
                "Select a season and click \u2018Generate Data\u2019 to fetch live data from FanGraphs."
            )
          )
        )
      }
    })

    output$table <- renderDT({
      dat <- spz_filtered()
      if (is.null(dat) || nrow(dat) == 0) return(NULL)
      render_spz_dt(dat)
    })

    # ── SP Skillz Compare tab ─────────────────────────────────────────────────

    spz_compare_pool <- reactive({
      dat <- spz_data()
      if (is.null(dat) || nrow(dat) == 0) return(character(0))
      sort(dat[["Player"]][nzchar(dat[["Player"]])])
    })

    mk_spz_slot <- function(i, pool = character(0), selected = "") {
      choices <- if (nzchar(selected)) c("", selected, setdiff(pool, selected)) else c("", pool)
      div(class = "sps-my-slot", id = ns(paste0("spz_slot_wrap_", i)),
        selectizeInput(ns(paste0("spz_p_", i)), label = NULL,
                       choices  = choices,
                       selected = selected,
                       multiple = FALSE,
                       options  = list(
                         placeholder = paste0("Pitcher ", i, "\u2026"),
                         maxItems    = 1L,
                         create      = FALSE,
                         onItemAdd   = I("function(v,$i){this.close();}")
                       ))
      )
    }

    output$compare_slots_ui <- renderUI({
      n_init <- isolate(max(1L, min(30L, as.integer(input$n_compare %||% 5L))))
      pool   <- isolate(spz_compare_pool())
      div(class = "sps-my-slots", id = ns("compare_slots_container"),
          tagList(lapply(seq_len(n_init), mk_spz_slot, pool = pool)))
    })

    prev_n_compare <- reactiveVal(5L)

    observeEvent(input$n_compare, {
      n_new <- max(1L, min(30L, as.integer(input$n_compare %||% 5L)))
      n_old <- prev_n_compare()
      pool  <- isolate(spz_compare_pool())
      if (n_new > n_old) {
        for (i in seq(n_old + 1L, n_new))
          insertUI(selector = paste0("#", ns("compare_slots_container")),
                   where    = "beforeEnd",
                   ui       = mk_spz_slot(i, pool))
      } else if (n_new < n_old) {
        for (i in seq(n_old, n_new + 1L))
          removeUI(selector = paste0("#", ns(paste0("spz_slot_wrap_", i))))
      }
      prev_n_compare(n_new)
    }, ignoreInit = TRUE)

    observeEvent(spz_compare_pool(), {
      pool <- spz_compare_pool()
      n    <- prev_n_compare()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("spz_p_", i), choices = c("", pool))
    }, ignoreInit = FALSE)

    observeEvent(input$clear_compare, {
      pool <- isolate(spz_compare_pool())
      n    <- prev_n_compare()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("spz_p_", i),
                             choices = c("", pool), selected = "")
    })

    compare_selected <- reactive({
      n   <- prev_n_compare()
      nms <- vapply(seq_len(n), function(i) {
        v <- input[[paste0("spz_p_", i)]]
        if (is.null(v) || !nzchar(v)) "" else v
      }, character(1))
      nms[nzchar(nms)]
    })

    output$compare_dt <- renderDT({
      msg_dt <- function(txt) datatable(
        data.frame(` ` = txt, check.names = FALSE),
        rownames = FALSE, options = list(dom = "t", ordering = FALSE)
      )
      dat_full <- spz_data_adp()
      if (is.null(dat_full) || nrow(dat_full) == 0)
        return(msg_dt("Generate SP Skillz data first, then select pitchers to compare."))
      sel <- compare_selected()
      if (!length(sel))
        return(msg_dt("Select pitchers above to compare them."))
      sel_lower <- tolower(trimws(sel))
      dat_lower <- tolower(trimws(dat_full[["Player"]]))
      sub <- dat_full[dat_lower %in% sel_lower, , drop = FALSE]
      if (nrow(sub) == 0)
        return(msg_dt("None of the selected pitchers were found in the SP Skillz data."))
      sub <- sub[order(sub[["RK"]]), , drop = FALSE]
      render_spz_dt(sub, full_scores = dat_full[["Score"]], dom_str = "t")
    })

    # ── External trigger (e.g. from Streamonator Fetch Probables) ────────────
    # Always re-fetches std + l30 when triggered (no cache guard).
    if (!is.null(fetch_trigger)) {
      observeEvent(fetch_trigger(), {
        req(fetch_trigger() > 0)
        yr <- as.integer(input$year %||% "2026")
        withProgress(message = "Fetching SP Skillz…", value = 0, {
          rv_spz$fetch_state   <- "none"
          rv_spz$savant_whiff  <- NULL
          if (yr == 2025L) rv_spz$gsheets_stf <- spz_fetch_gsheets_stf()
          incProgress(0.1, detail = "Savant whiff data…")
          rv_spz$savant_whiff <- spz_fetch_savant_whiff(yr)
          fetch_raw_ext <- function(month) {
            tryCatch({
              url <- spz_gen_build_url(season = yr, month = month)
              res <- spz_gen_fetch(url)
              spz_gen_parse(res)
            }, error = function(e) NULL)
          }
          incProgress(0.4, detail = "Season to Date…")
          rv_spz$raw_std <- fetch_raw_ext(0L)
          incProgress(0.5, detail = "Last 30 Days…")
          rv_spz$raw_l30 <- fetch_raw_ext(3L)
          incProgress(0.1)
          raw_std <- rv_spz$raw_std
          if (!is.null(raw_std) && nrow(raw_std) > 0) {
            rv_spz$status      <- sprintf("%d pitchers — %s", nrow(raw_std), format(Sys.time(), "%I:%M %p"))
            rv_spz$fetch_state <- "ok"
          } else {
            rv_spz$status      <- paste0("Fetch failed — ", format(Sys.time(), "%I:%M %p"))
            rv_spz$fetch_state <- "error"
          }
        })
      }, ignoreInit = TRUE)
    }
    # ── Return reactive data for Streamonator consumption ─────────────────────
    return(reactive({ list(std = rv_spz$std, l30 = rv_spz$l30) }))

  })
}
