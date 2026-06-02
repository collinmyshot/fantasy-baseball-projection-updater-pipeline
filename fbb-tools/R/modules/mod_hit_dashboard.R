suppressPackageStartupMessages({ library(DT) })

# ── Constants ──────────────────────────────────────────────────────────────────

HD_SV_CUSTOM    <- "https://baseballsavant.mlb.com/leaderboard/custom"
HD_SV_BATBALL   <- "https://baseballsavant.mlb.com/leaderboard/batted-ball"
HD_SV_BATTRACK  <- "https://baseballsavant.mlb.com/leaderboard/bat-tracking"
HD_FG_BASE      <- "https://www.fangraphs.com/api/leaders/major-league/data"
HD_FG_UA        <- paste0(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Savant custom leaderboard selections (confirmed field names)
HD_SV_SELECTIONS <- paste0(
  "pa,home_run,batting_avg,slg_percent,babip,xba,xslg,bacon,",
  "barrel,barrel_batted_rate,hard_hit_percent,groundballs_percent,",
  "k_percent,bb_percent,whiff_percent,oz_swing_percent,",
  "oz_contact_percent,iz_contact_percent,pull_percent"
)

HD_SEASONS        <- c("2024", "2025", "2026")
HD_DEFAULT_MIN_PA <- 20L

# Column order matches display grouping:
#   Identity | Luck Stats | Power Stats | Plate Skills
HD_COLS <- data.frame(
  id    = c(
    "player_name", "team_abbrev", "pa",
    # Luck Stats
    "avg", "xba", "slg", "xslg", "bacon", "babip", "hr_barrel_pct",
    # Power Stats
    "barrel_bbe_pct", "barrel_pa_pct", "abrl_bbe_pct", "abrl_pa_pct",
    "hard_hit_pct",
    "pull_pct", "pull_air_pct", "gb_pct", "bat_speed",
    # Plate Skills
    "k_pct", "bb_pct", "swstr_pct",
    "o_swing_pct", "z_swing_pct", "w_swing_pct", "z_con_pct"
  ),
  label = c(
    "Player", "Team", "PA",
    "AVG", "xBA", "SLG", "xSLG", "BACON", "BABIP", "HR/Barrel",
    "Barrel% (BBE)", "Barrel% (PA)", "aBrl% (BBE)", "aBrl% (PA)",
    "Hard Hit%",
    "Pull%", "Pull Air%", "GB%", "Bat Speed",
    "K%", "BB%", "SwStr%",
    "O-Swing%", "Z-Swing%", "W-Swing%", "Z-Con%"
  ),
  dir   = c(
     0,  0,  0,
     1,  1,  1,  1,  1,  1,  0,
     1,  1,  1,  1,  1,
     0,  0, -1,  1,
    -1,  1, -1, -1,  1, -1,  1
  ),
  pct   = c(
    FALSE, FALSE, FALSE,
    FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, TRUE,
    TRUE, TRUE, TRUE, TRUE, TRUE,
    TRUE, TRUE, TRUE, FALSE,
    TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE
  ),
  stringsAsFactors = FALSE
)

# Column IDs always shown (never in stat selector)
HD_ID_COLS <- c("player_name", "team_abbrev", "pa")

# Stat columns unchecked by default in the selector
HD_DEFAULT_UNCHECKED <- c("babip", "barrel_pa_pct", "abrl_pa_pct")

# Columns used for PA-weighted league average (exclude identity cols)
HD_RATE_COLS <- HD_COLS$id[!HD_COLS$id %in% HD_ID_COLS]

# Stat groups for selector (in display order)
HD_STAT_GROUPS <- list(
  luck = list(
    label = "Luck Stats",
    ids   = c("avg", "xba", "slg", "xslg", "bacon", "babip", "hr_barrel_pct")
  ),
  power = list(
    label = "Power Stats",
    ids   = c("barrel_bbe_pct", "barrel_pa_pct", "abrl_bbe_pct", "abrl_pa_pct",
              "hard_hit_pct",
              "pull_pct", "pull_air_pct", "gb_pct", "bat_speed")
  ),
  plate = list(
    label = "Plate Skills",
    ids   = c("k_pct", "bb_pct", "swstr_pct",
              "o_swing_pct", "z_swing_pct", "w_swing_pct", "z_con_pct")
  )
)

# ── Data fetchers ──────────────────────────────────────────────────────────────

# Generic Savant CSV fetch — download.file (libcurl, no shell escaping issues)
.hd_sv_read <- function(url) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)

  ok <- tryCatch({
    download.file(url, tmp, quiet = TRUE, method = "libcurl",
                  headers = c("Accept" = "text/csv, */*",
                              "User-Agent" = HD_FG_UA))
    TRUE
  }, error = function(e) {
    tryCatch({ download.file(url, tmp, quiet = TRUE, method = "auto"); TRUE },
             error = function(e2) FALSE)
  })
  if (!ok) return(NULL)

  # Read lines, strip UTF-8 BOM if present, parse with proper quoting
  tryCatch({
    lines <- readLines(tmp, warn = FALSE)
    if (length(lines) == 0) return(NULL)
    lines[1] <- sub("^\xef\xbb\xbf", "", lines[1])  # strip BOM
    read.csv(text = paste(lines, collapse = "\n"),
             stringsAsFactors = FALSE, check.names = FALSE, quote = "\"")
  }, error = function(e) NULL)
}

# Shared parser for Savant custom CSV → dashboard data frame
.parse_sv_custom_raw <- function(raw) {
  if (is.null(raw)) return(NULL)

  # The combined name column header literally contains a comma: `"last_name, first_name"`
  name_col <- grep("last_name", names(raw), value = TRUE)[1]
  if (is.na(name_col)) return(NULL)

  # Reconstruct "First Last" from "Last, First"
  parts <- strsplit(raw[[name_col]], ", ", fixed = TRUE)
  raw$player_name <- vapply(parts, function(p) {
    if (length(p) == 2L) paste(trimws(p[2]), trimws(p[1])) else trimws(p[1])
  }, character(1))
  raw$name_key <- player_nk(raw$player_name)

  cn  <- names(raw)
  num <- function(col) {
    if (col %in% cn) suppressWarnings(as.numeric(raw[[col]])) else rep(NA_real_, nrow(raw))
  }

  out <- data.frame(
    player_id      = suppressWarnings(as.integer(raw[["player_id"]])),
    player_name    = raw$player_name,
    name_key       = raw$name_key,
    pa             = num("pa"),
    hr_count       = num("home_run"),
    avg            = num("batting_avg"),
    slg            = num("slg_percent"),
    babip          = num("babip"),
    xba            = num("xba"),
    xslg           = num("xslg"),
    bacon          = num("bacon"),
    barrel_count   = num("barrel"),
    barrel_bbe_pct = num("barrel_batted_rate"),
    hard_hit_pct   = num("hard_hit_percent"),
    gb_pct         = num("groundballs_percent"),
    k_pct          = num("k_percent"),
    bb_pct         = num("bb_percent"),
    whiff_pct      = num("whiff_percent"),
    o_swing_pct    = num("oz_swing_percent"),
    o_con_pct      = num("oz_contact_percent"),
    z_con_pct      = num("iz_contact_percent"),
    pull_pct       = num("pull_percent"),
    stringsAsFactors = FALSE
  )

  out$barrel_pa_pct <- ifelse(out$pa > 0, out$barrel_count / out$pa * 100, NA_real_)
  out$hr_barrel_pct <- ifelse(
    !is.na(out$barrel_count) & out$barrel_count > 0 & !is.na(out$hr_count),
    out$hr_count / out$barrel_count * 100,
    NA_real_
  )
  out
}

# Savant custom leaderboard → most hitting metrics (single season)
fetch_sv_custom <- function(year) {
  url <- paste0(
    HD_SV_CUSTOM,
    "?year=", year,
    "&type=batter&filter=&min=1",
    "&selections=", utils::URLencode(HD_SV_SELECTIONS, reserved = TRUE),
    "&chart=false&csv=true"
  )
  .parse_sv_custom_raw(.hd_sv_read(url))
}


# Savant batted-ball leaderboard → pull_air_rate
fetch_sv_batball <- function(year) {
  url <- paste0(
    HD_SV_BATBALL,
    "?gameType=Regular&minSwings=1&minGroupSwings=1",
    "&seasonStart=", year, "&seasonEnd=", year,
    "&type=batter&csv=true"
  )
  raw <- .hd_sv_read(url)
  if (is.null(raw) || !"id" %in% names(raw)) return(NULL)

  data.frame(
    player_id    = suppressWarnings(as.integer(raw[["id"]])),
    pull_air_pct = suppressWarnings(as.numeric(raw[["pull_air_rate"]])) * 100,
    stringsAsFactors = FALSE
  )
}


# Savant bat tracking leaderboard → avg_bat_speed
fetch_sv_battracking <- function(year) {
  url <- paste0(
    HD_SV_BATTRACK,
    "?gameType=Regular&minSwings=1",
    "&year=", year,
    "&csv=true"
  )
  raw <- .hd_sv_read(url)
  if (is.null(raw) || !"id" %in% names(raw)) return(NULL)

  data.frame(
    player_id = suppressWarnings(as.integer(raw[["id"]])),
    bat_speed = suppressWarnings(as.numeric(raw[["avg_bat_speed"]])),
    stringsAsFactors = FALSE
  )
}

# ── FanGraphs fetch (plate discipline + zone stats) ───────────────────────────

.hd_fg_fetch <- function(url) {
  fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
}

# FanGraphs plate discipline (type=7): Z-Swing%, O-Swing%, Z-Con%, O-Con%, SwStr%
# FanGraphs Statcast zone (type=23): W-Swing%  [speculative — graceful failure]
fetch_fg_discipline <- function(year) {
  fg_url <- function(type) paste0(
    HD_FG_BASE,
    "?pos=all&stats=bat&lg=all&qual=0&ind=0",
    "&team=0&rost=0&players=0",
    "&type=", type,
    "&season=", year, "&season1=", year,
    "&pageitems=2000&pagenum=1"
  )

  # Helper: parse FG JSON payload into a data frame
  parse_fg <- function(res) {
    if (!isTRUE(res$ok)) return(NULL)
    p  <- res$payload
    df <- if (is.data.frame(p))       p
          else if (is.data.frame(p$data)) p$data
          else NULL
    if (is.null(df) || nrow(df) == 0) return(NULL)
    df
  }

  # Robust column extractor (case-insensitive name match)
  col_num <- function(df, ...) {
    cn <- tolower(names(df))
    for (nm in tolower(c(...))) {
      idx <- match(nm, cn)
      if (!is.na(idx)) return(suppressWarnings(as.numeric(df[[idx]])))
    }
    rep(NA_real_, nrow(df))
  }

  col_chr <- function(df, ...) {
    cn <- tolower(names(df))
    for (nm in tolower(c(...))) {
      idx <- match(nm, cn)
      if (!is.na(idx)) return(as.character(df[[idx]]))
    }
    rep(NA_character_, nrow(df))
  }

  # Plate discipline fetch (type=7)
  disc_res <- .hd_fg_fetch(fg_url(7))
  disc_df  <- parse_fg(disc_res)

  # Strip HTML anchor tags from a character vector
  strip_html <- function(x) trimws(gsub("<[^>]+>", "", x))

  out <- NULL
  if (!is.null(disc_df)) {
    # FG embeds player name + team in HTML anchors; xMLBAMID is the MLBAM join key
    mlbam_id    <- suppressWarnings(as.integer(col_chr(disc_df, "xMLBAMID", "xmlbamid", "mlbamid")))
    raw_names   <- strip_html(col_chr(disc_df, "Name", "PlayerName", "name"))
    raw_team    <- strip_html(col_chr(disc_df, "Team", "TeamName", "teamname"))

    # FG returns plate discipline as 0-1 fractions; multiply by 100 for pct scale
    out <- data.frame(
      player_id   = mlbam_id,
      name_key    = player_nk(raw_names),
      team_abbrev = raw_team,
      z_swing_pct = col_num(disc_df, "Z-Swing%", "z-swing%") * 100,
      o_swing_pct = col_num(disc_df, "O-Swing%", "o-swing%") * 100,
      z_con_pct   = col_num(disc_df, "Z-Contact%", "z-con%") * 100,
      o_con_pct   = col_num(disc_df, "O-Contact%", "o-con%") * 100,
      swstr_pct   = col_num(disc_df, "SwStr%",     "swstr%") * 100,
      stringsAsFactors = FALSE
    )

    # scW-Swing% = Statcast waste-zone swing%; falls back to W-Swing% if unavailable
    wswing_idx <- grep("^scw-swing%$|^scw.swing", tolower(names(disc_df)))[1]
    if (is.na(wswing_idx)) wswing_idx <- grep("^w-swing%$|^w.swing%", tolower(names(disc_df)))[1]
    out$w_swing_pct <- if (!is.na(wswing_idx)) {
      suppressWarnings(as.numeric(disc_df[[names(disc_df)[wswing_idx]]])) * 100
    } else {
      NA_real_
    }
  }

  out
}


# ── aBrl data loader (reuses the same CSV as aBrl leaderboard) ────────────────

load_abrl_for_dashboard <- function(year) {
  path <- file.path("data", "processed", "adjusted_barrel_hitters.csv")
  if (!file.exists(path)) return(NULL)
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  d <- d[d$season == as.integer(year), , drop = FALSE]
  if (nrow(d) == 0) return(NULL)
  data.frame(
    player_id    = suppressWarnings(as.integer(d$batter)),
    abrl_bbe_pct = round(as.numeric(d$adj_brl_pct), 1),
    abrl_pa_pct  = ifelse(d$pa > 0, round(d$adj_barrels / d$pa * 100, 1), NA_real_),
    stringsAsFactors = FALSE
  )
}

# ── Build combined data frame ──────────────────────────────────────────────────

build_hit_dashboard <- function(sv_custom, sv_batball, sv_battrack, fg_disc, abrl = NULL) {
  if (is.null(sv_custom) || nrow(sv_custom) == 0) return(NULL)

  df <- sv_custom

  # Join Pull Air% from batted-ball leaderboard
  if (!is.null(sv_batball) && nrow(sv_batball) > 0) {
    idx <- match(df$player_id, sv_batball$player_id)
    df$pull_air_pct <- sv_batball$pull_air_pct[idx]
  } else {
    df$pull_air_pct <- NA_real_
  }

  # Join bat speed from bat tracking leaderboard
  if (!is.null(sv_battrack) && nrow(sv_battrack) > 0) {
    idx <- match(df$player_id, sv_battrack$player_id)
    df$bat_speed <- sv_battrack$bat_speed[idx]
  } else {
    df$bat_speed <- NA_real_
  }

  # Join FanGraphs plate discipline + zone (primary: MLBAM player_id; fallback: name_key)
  if (!is.null(fg_disc) && nrow(fg_disc) > 0) {
    idx <- match(df$player_id, fg_disc$player_id)
    unmatched <- is.na(idx)
    if (any(unmatched)) {
      idx[unmatched] <- match(df$name_key[unmatched], fg_disc$name_key)
    }

    df$z_swing_pct <- fg_disc$z_swing_pct[idx]
    df$swstr_pct   <- fg_disc$swstr_pct[idx]
    df$w_swing_pct <- fg_disc$w_swing_pct[idx]
    fg_team        <- fg_disc$team_abbrev[idx]
    df$team_abbrev <- ifelse(!is.na(fg_team) & nzchar(fg_team), fg_team, NA_character_)
    sv_o  <- if ("o_swing_pct" %in% names(df)) df$o_swing_pct else NA_real_
    sv_zc <- if ("z_con_pct"   %in% names(df)) df$z_con_pct   else NA_real_
    df$o_swing_pct <- ifelse(is.na(sv_o),  fg_disc$o_swing_pct[idx], sv_o)
    df$z_con_pct   <- ifelse(is.na(sv_zc), fg_disc$z_con_pct[idx],   sv_zc)
  } else {
    df$z_swing_pct <- NA_real_
    df$swstr_pct   <- NA_real_
    df$w_swing_pct <- NA_real_
    if (!"team_abbrev" %in% names(df)) df$team_abbrev <- NA_character_
  }

  # Join aBrl rates from pre-built CSV
  if (!is.null(abrl) && nrow(abrl) > 0) {
    idx <- match(df$player_id, abrl$player_id)
    df$abrl_bbe_pct <- abrl$abrl_bbe_pct[idx]
    df$abrl_pa_pct  <- abrl$abrl_pa_pct[idx]
  } else {
    df$abrl_bbe_pct <- NA_real_
    df$abrl_pa_pct  <- NA_real_
  }

  # Ensure all expected columns exist
  for (col_id in HD_COLS$id) {
    if (!col_id %in% names(df)) df[[col_id]] <- NA_real_
  }

  df[, HD_COLS$id, drop = FALSE]
}

# ── League average row ────────────────────────────────────────────────────────

compute_league_avg <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  pa <- df$pa
  pa[is.na(pa) | pa <= 0] <- 0

  la <- data.frame(
    player_name = "── League Avg ──",
    team_abbrev = "",
    stringsAsFactors = FALSE
  )
  # Mean PA across included players (not sum)
  la$pa <- round(mean(pa[pa > 0], na.rm = TRUE))

  for (col in HD_RATE_COLS) {
    if (!col %in% names(df)) { la[[col]] <- NA_real_; next }
    vals  <- suppressWarnings(as.numeric(df[[col]]))
    valid <- !is.na(vals) & pa > 0
    if (!any(valid)) { la[[col]] <- NA_real_; next }
    la[[col]] <- stats::weighted.mean(vals[valid], pa[valid], na.rm = TRUE)
  }
  la
}

# ── DT rendering (shared by leaderboard + compare) ────────────────────────────

# Shared DT builder for both leaderboard and compare tabs.
# df:            raw numeric player rows (HD_COLS columns only — no extra cols)
# la:            raw numeric league avg row (NULL for compare)
# compare:       TRUE = no LA, no length menu, no fixed cols, no scrollY
# selected_cols: character vector of stat col IDs to show (NULL = all)
render_hit_dashboard_dt <- function(df, la = NULL, compare = FALSE, selected_cols = NULL) {
  if (is.null(df) || nrow(df) == 0)
    return(DT::datatable(data.frame(Message = "No data available.")))

  # Resolve which columns to show (identity cols always included)
  if (!is.null(selected_cols)) {
    keep_ids  <- c(HD_ID_COLS, intersect(HD_COLS$id, selected_cols))
    show_cols <- HD_COLS[HD_COLS$id %in% keep_ids, , drop = FALSE]
    # Preserve HD_COLS display order
    show_cols <- show_cols[order(match(show_cols$id, HD_COLS$id)), , drop = FALSE]
  } else {
    show_cols <- HD_COLS
  }

  # Subset df to shown columns
  df_show <- df[, show_cols$id, drop = FALSE]

  n_show  <- nrow(show_cols)
  col_idx <- setNames(seq_len(n_show) - 1L, show_cols$id)
  pa_col  <- col_idx[["pa"]]

  # .row_order is appended as the last column (hidden; drives LA pinning)
  row_order_ci <- n_show  # 0-indexed position of .row_order column

  # Build combined data: LA row first (row_order=0), then players (row_order=1..n)
  if (!compare && !is.null(la) && nrow(la) > 0) {
    la_show                <- la[1, show_cols$id, drop = FALSE]
    la_show$.row_order     <- 0L
    df_plot                <- df_show
    df_plot$.row_order     <- seq_len(nrow(df_show))
    dt_data <- rbind(la_show, df_plot)
  } else {
    df_plot            <- df_show
    df_plot$.row_order <- seq_len(nrow(df_show))
    dt_data <- df_plot
  }

  # Group boundary cols — first col of each stat group gets a left border
  grp_starts <- intersect(c("avg", "barrel_bbe_pct", "k_pct"), show_cols$id)

  # ── JS render functions (return raw value for sort; format for display) ────
  r_pct <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(1)+'%';
  }")
  r_bat <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(3).replace(/^0\\./, '.');
  }")
  r_int <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return Math.round(+d).toString();
  }")
  r_dec <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(1);
  }")

  # ── Per-column defs ────────────────────────────────────────────────────────
  col_defs <- lapply(seq_len(n_show), function(i) {
    col <- show_cols$id[i]
    pct <- show_cols$pct[i]
    ci  <- col_idx[[col]]
    aln <- if (col == "player_name") "dt-left" else "dt-center"
    bdr <- if (col %in% grp_starts)  "hd-grp-start" else ""
    cls <- trimws(paste(aln, bdr))

    # Text columns: no numeric render or type
    if (col %in% c("player_name", "team_abbrev")) {
      return(list(targets = ci, className = cls))
    }
    rnd <- if (pct)                 r_pct
           else if (col == "pa")        r_int
           else if (col == "bat_speed") r_dec
           else                         r_bat
    list(targets = ci, className = cls, type = "num", render = rnd)
  })

  # Hide .row_order column
  col_defs <- c(col_defs, list(list(targets = row_order_ci, visible = FALSE)))

  # ── Row callback: style the LA row (row_order === 0) ─────────────────────
  la_row_cb <- DT::JS(sprintf(
    "function(row, data, index) {
      if (data[%d] === 0) {
        $(row).css({
          'background-color': '#1f3556',
          'color': '#fff',
          'font-weight': 'bold',
          'border-top': '2px solid #0d2240',
          'border-bottom': '2px solid #0d2240'
        });
      }
    }",
    row_order_ci
  ))

  # ── Column group membership ────────────────────────────────────────────────
  luck_ids  <- HD_STAT_GROUPS$luck$ids
  power_ids <- HD_STAT_GROUPS$power$ids
  plate_ids <- HD_STAT_GROUPS$plate$ids
  stat_ids  <- show_cols$id[!show_cols$id %in% HD_ID_COLS]
  luck_n    <- sum(stat_ids %in% luck_ids)
  power_n   <- sum(stat_ids %in% power_ids)
  plate_n   <- sum(stat_ids %in% plate_ids)

  # ── Column label styles ────────────────────────────────────────────────────
  luck_bg  <- "background:rgba(46,109,164,0.07);"
  power_bg <- "background:rgba(42,122,75,0.07);"
  plate_bg <- "background:rgba(122,59,110,0.07);"

  mk_col_th <- function(i) {
    col <- show_cols$id[i]
    lbl <- show_cols$label[i]
    bg  <- if (col %in% luck_ids)       luck_bg
           else if (col %in% power_ids) power_bg
           else if (col %in% plate_ids) plate_bg
           else ""
    bdr <- if (col %in% grp_starts) "border-left:3px solid rgba(100,100,100,0.2);" else ""
    tags$th(lbl, style = paste0(bg, bdr))
  }

  # ── Build group header row (row 1 of thead) ────────────────────────────────
  base_style <- paste0(
    "text-align:center;font-size:0.72rem;font-weight:700;",
    "letter-spacing:.05em;text-transform:uppercase;color:#fff;padding:5px 6px;"
  )
  sep_style <- "border-left:3px solid rgba(255,255,255,0.35);"

  grp_order <- c("luck", "power", "plate")
  grp_ths <- list(
    # Identity columns spacer
    tags$th(colspan = length(HD_ID_COLS),
            style = "border:none;background:transparent;")
  )
  first_stat <- TRUE
  for (g in grp_order) {
    n <- switch(g, luck = luck_n, power = power_n, plate = plate_n)
    if (n == 0) next
    sep <- if (!first_stat) sep_style else ""
    first_stat <- FALSE
    bg_color <- switch(g, luck = "#2e6da4", power = "#2a7a4b", plate = "#7a3b6e")
    grp_ths <- c(grp_ths, list(
      tags$th(HD_STAT_GROUPS[[g]]$label,
              colspan = n,
              style   = paste0(base_style, sep, "background:", bg_color, ";"))
    ))
  }
  # Hidden .row_order column spacer (must match total column count)
  grp_ths <- c(grp_ths, list(tags$th(style = "display:none;", "")))

  # ── Two-row container: group row + column name row ─────────────────────────
  grp_tr <- do.call(tags$tr, c(list(class = "hd-grp-hdr"), grp_ths))
  col_tr <- do.call(tags$tr, c(
    lapply(seq_len(n_show), mk_col_th),
    list(tags$th(style = "display:none;", ""))
  ))
  container <- htmltools::withTags(table(
    class = "display",
    thead(grp_tr, col_tr)
  ))

  # ── Options ────────────────────────────────────────────────────────────────
  if (compare) {
    opts <- list(
      pageLength = nrow(df_show),
      dom        = "t",
      ordering   = FALSE,
      scrollX    = TRUE,
      autoWidth  = FALSE,
      columnDefs = col_defs
    )
    exts <- character(0)
  } else {
    # initComplete: disable sorting on group header row
    init_cb <- DT::JS(
      "function(settings, json) {",
      "  var $thead = $(this.api().table().header());",
      "  $thead.find('tr.hd-grp-hdr th').off('click.DT').css('cursor','default');",
      "}"
    )
    opts <- list(
      pageLength      = 50,
      lengthMenu      = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
      scrollX         = TRUE,
      autoWidth       = FALSE,
      orderCellsTop   = FALSE,
      dom             = "lrtip",
      order           = list(list(pa_col, "desc")),
      orderFixed      = list(list(row_order_ci, "asc")),
      columnDefs      = col_defs,
      rowCallback     = la_row_cb,
      initComplete    = init_cb
    )
    exts <- character(0)
  }

  DT::datatable(
    dt_data,
    rownames   = FALSE,
    container  = container,
    class      = "pf-dt display compact nowrap",
    filter     = "none",
    extensions = exts,
    options    = opts
  )
}

# ── Compare table: transposed (stats = rows, players = columns) ───────────────

render_hit_dashboard_compare_dt <- function(sel_df, selected_cols = NULL) {
  if (is.null(sel_df) || nrow(sel_df) == 0)
    return(DT::datatable(data.frame(Message = "No data available.")))

  # Resolve which columns to show
  if (!is.null(selected_cols)) {
    keep_ids  <- c(HD_ID_COLS, intersect(HD_COLS$id, selected_cols))
    show_cols <- HD_COLS[HD_COLS$id %in% keep_ids, , drop = FALSE]
    show_cols <- show_cols[order(match(show_cols$id, HD_COLS$id)), , drop = FALSE]
  } else {
    show_cols <- HD_COLS
  }

  # Stat rows (non-identity columns only)
  stat_cols <- show_cols[!show_cols$id %in% c("player_name", "team_abbrev"), , drop = FALSE]
  players   <- sel_df$player_name

  # Format a single numeric value for display
  fmt_val <- function(x, is_pct, col_id) {
    if (is.na(x) || !is.finite(x)) return("")
    if (col_id == "pa")        return(as.character(round(x)))
    if (col_id == "bat_speed") return(sprintf("%.1f", x))
    if (is_pct)                return(sprintf("%.1f%%", x))
    return(sub("^0\\.", ".", sprintf("%.3f", x)))
  }

  # Build one row per stat, highlighting the best value(s)
  out_rows <- lapply(seq_len(nrow(stat_cols)), function(i) {
    col_id <- stat_cols$id[i]
    lbl    <- stat_cols$label[i]
    is_pct <- stat_cols$pct[i]
    direction <- stat_cols$dir[i]
    vals <- vapply(players, function(pname) {
      v <- sel_df[sel_df$player_name == pname, col_id, drop = TRUE]
      if (length(v) == 0) NA_real_ else suppressWarnings(as.numeric(v[1]))
    }, numeric(1))
    formatted <- vapply(vals, fmt_val, character(1), is_pct = is_pct, col_id = col_id)

    # Highlight best value(s) when direction is known and >1 player
    if (direction != 0 && sum(!is.na(vals)) > 1) {
      best_val <- if (direction > 0) max(vals, na.rm = TRUE) else min(vals, na.rm = TRUE)
      # "Within rounding error" tolerance: values that round to the same display
      tol <- if (col_id %in% c("pa", "bat_speed")) 0.5
             else if (is_pct) 0.05
             else 0.0005
      is_best <- !is.na(vals) & abs(vals - best_val) <= tol
      formatted[is_best] <- paste0(
        '<span style="background:#d4edda;padding:2px 6px;border-radius:4px;font-weight:700;">',
        formatted[is_best], '</span>'
      )
    }

    as.data.frame(
      c(list(Stat = lbl), setNames(as.list(formatted), players)),
      stringsAsFactors = FALSE, check.names = FALSE
    )
  })

  transposed <- do.call(rbind, out_rows)

  # Row background color by stat group
  luck_labels  <- HD_COLS$label[HD_COLS$id %in% HD_STAT_GROUPS$luck$ids]
  power_labels <- HD_COLS$label[HD_COLS$id %in% HD_STAT_GROUPS$power$ids]

  row_cb <- DT::JS(sprintf(
    "function(row, data, index) {
      var stat  = data[0];
      var luck  = %s;
      var power = %s;
      if (luck.indexOf(stat)  >= 0) {
        $(row).css('background-color','rgba(46,109,164,0.07)');
      } else if (power.indexOf(stat) >= 0) {
        $(row).css('background-color','rgba(42,122,75,0.07)');
      } else {
        $(row).css('background-color','rgba(122,59,110,0.07)');
      }
    }",
    jsonlite::toJSON(luck_labels),
    jsonlite::toJSON(power_labels)
  ))

  DT::datatable(
    transposed,
    rownames   = FALSE,
    escape     = FALSE,
    class      = "pf-dt display compact",
    extensions = character(0),
    options    = list(
      pageLength  = nrow(transposed),
      dom         = "t",
      ordering    = FALSE,
      scrollX     = TRUE,
      rowCallback = row_cb,
      columnDefs  = list(
        list(targets = 0,      className = "dt-left", width = "120px"),
        list(targets = "_all", className = "dt-center")
      )
    )
  )
}

# ── Shiny UI ──────────────────────────────────────────────────────────────────

hitDashUI <- function(id) {
  ns <- NS(id)

  div(
    class = "hd-page",
    tags$style(HTML("
      .hd-grp-start { border-left: 3px solid rgba(80,80,80,0.18) !important; }

      /* Stat selector: three side-by-side cards */
      .hd-selector-row {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin: 14px 0 6px;
      }
      .hd-selector-card {
        flex: 1 1 200px;
        background: var(--card, #fff);
        border: 1px solid var(--line, #dee2e6);
        border-radius: var(--r-md, 6px);
        padding: 12px 16px 10px;
      }
      .hd-selector-card-title {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 8px;
        padding-bottom: 6px;
        border-bottom: 1px solid var(--line, #dee2e6);
      }
      .hd-selector-card-luck  .hd-selector-card-title { color: #2e6da4; }
      .hd-selector-card-power .hd-selector-card-title { color: #2a7a4b; }
      .hd-selector-card-plate .hd-selector-card-title { color: #7a3b6e; }
      .hd-selector-card .shiny-input-container { margin-bottom: 0; }
      .hd-selector-card .checkbox-inline {
        display: block;
        margin: 0 0 4px 0;
        font-size: 0.82rem;
        color: var(--text, #212529);
      }
      .hd-selector-card .checkbox-inline input[type=checkbox] { margin-right: 5px; }
    ")),

    # Page header
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "In-Season Tools"),
      h1(class = "pf-title", "Hitter Dashboard"),
      p(class = "pf-subtitle",
        "Plate discipline, batted-ball profile, and expected stats for all qualified hitters.",
        tags$br(),
        tags$span(class = "text-muted",
          "Sources: ",
          tags$b("Statcast"), " · ",
          tags$b("FanGraphs")
        )
      )
    ),

    # Season + Min PA controls
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season"),
        div(class = "pf-toggle",
            radioButtons(ns("season"), label = NULL,
                         choices  = setNames(HD_SEASONS, HD_SEASONS),
                         selected = HD_SEASONS[length(HD_SEASONS)],
                         inline   = TRUE))
      ),
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Min PA"),
        numericInput(ns("min_pa"), label = NULL,
                     value = HD_DEFAULT_MIN_PA, min = 1, max = 9999, step = 1,
                     width = "80px")
      )
    ),

    # Fetch row
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch Hitter Data",
                   class = "btn btn-pag-generate",
                   icon  = icon("rotate-right")),
      div(class = "sps-status status-shell", uiOutput(ns("fetch_status"), inline = TRUE))
    ),

    # Stat selector (shown after data is loaded)
    uiOutput(ns("stat_selector_ui")),

    # Sub-tabs
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4CA"), "Leaderboard"),
        value = "leaderboard",
        div(
          class = "sps-tab-body",
          div(
            class = "pf-controls-row spz-search-row",
            div(
              class = "spz-search-wrap",
              tags$span(class = "spz-search-icon", HTML("&#x2315;")),
              textInput(ns("search"), label = NULL,
                        placeholder = "Search player or team…", width = "100%")
            )
          ),
          uiOutput(ns("table_ui"))
        )
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F9FE"), "Compare"),
        value = "compare",
        div(
          class = "sps-tab-body",
          style = "padding-bottom: 100px;",
          div(
            class = "sps-my-wrap",
            style = "max-width:640px;margin-bottom:16px;",
            selectizeInput(
              ns("compare_players"),
              label    = NULL,
              choices  = NULL,
              multiple = TRUE,
              options  = list(
                placeholder = "Search for players to compare…",
                maxItems    = 8,
                plugins     = list("remove_button")
              )
            )
          ),
          uiOutput(ns("compare_ui"))
        )
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4C8"), "Player Trends"),
        value = "trends",
        div(
          class = "sps-tab-body",

          # Controls row
          div(class = "spz-preset-row",
            style = "flex-wrap: wrap; gap: 8px 0;",
            div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
              tags$span(class = "spz-preset-label", "PLAYER 1"),
              selectizeInput(ns("trend_player1"), label = NULL,
                             choices = NULL, width = "220px",
                             options = list(placeholder = "Type a player name..."))
            ),
            div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
              tags$span(class = "spz-preset-label", "PLAYER 2"),
              selectizeInput(ns("trend_player2"), label = NULL,
                             choices = NULL, width = "220px",
                             options = list(placeholder = "(optional)"))
            )
          ),

          # Metric toggles (grouped)
          div(class = "hd-selector-row",
            div(class = "hd-selector-card hd-selector-card-luck",
              div(class = "hd-selector-card-title", "Luck Stats"),
              checkboxGroupInput(ns("trend_luck"), label = NULL, inline = FALSE,
                choices = setNames(HD_STAT_GROUPS$luck$ids,
                                   HD_COLS$label[match(HD_STAT_GROUPS$luck$ids, HD_COLS$id)]),
                selected = character(0))
            ),
            div(class = "hd-selector-card hd-selector-card-power",
              div(class = "hd-selector-card-title", "Power Stats"),
              checkboxGroupInput(ns("trend_power"), label = NULL, inline = FALSE,
                choices = setNames(HD_STAT_GROUPS$power$ids,
                                   HD_COLS$label[match(HD_STAT_GROUPS$power$ids, HD_COLS$id)]),
                selected = "barrel_bbe_pct")
            ),
            div(class = "hd-selector-card hd-selector-card-plate",
              div(class = "hd-selector-card-title", "Plate Skills"),
              checkboxGroupInput(ns("trend_plate"), label = NULL, inline = FALSE,
                choices = setNames(HD_STAT_GROUPS$plate$ids,
                                   HD_COLS$label[match(HD_STAT_GROUPS$plate$ids, HD_COLS$id)]),
                selected = character(0))
            )
          ),

          # Chart
          div(
            style = "margin-top: 12px; background: #fff; border: 1px solid #c9d7c5;
                     border-radius: 12px; padding: 16px 20px 18px;",
            uiOutput(ns("trend_chart_ui"))
          ),

          # Detail table
          div(style = "margin-top: 16px;",
            uiOutput(ns("trend_table_ui"))
          )
        )
      )
    )
  )
}

# ── Shiny server ──────────────────────────────────────────────────────────────

hitDashServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      data     = NULL,
      all_data = list(),
      error    = NULL,
      loading  = FALSE
    )

    observeEvent(input$fetch, {
      rv$loading <- TRUE
      rv$error   <- NULL
      rv$data    <- NULL

      year <- input$season

      withProgress(message = "Fetching hitter data…", value = 0, {

        setProgress(0.1, detail = "Savant custom leaderboard…")
        sv_custom <- tryCatch(fetch_sv_custom(year), error = function(e) {
          rv$error <- paste("Savant fetch failed:", conditionMessage(e))
          NULL
        })

        if (is.null(sv_custom)) {
          rv$loading <- FALSE
          return()
        }

        setProgress(0.35, detail = "Savant batted-ball leaderboard…")
        sv_batball <- tryCatch(fetch_sv_batball(year), error = function(e) NULL)

        setProgress(0.5, detail = "Savant bat tracking…")
        sv_battrack <- tryCatch(fetch_sv_battracking(year), error = function(e) NULL)

        setProgress(0.65, detail = "FanGraphs plate discipline…")
        fg_disc <- tryCatch(fetch_fg_discipline(year), error = function(e) NULL)

        setProgress(0.85, detail = "Building dashboard…")
        abrl <- tryCatch(load_abrl_for_dashboard(year), error = function(e) NULL)
        df_full <- build_hit_dashboard(sv_custom, sv_batball, sv_battrack, fg_disc, abrl)

        if (is.null(df_full) || nrow(df_full) == 0) {
          rv$error   <- "No data returned. Try a different season or lower the PA filter."
          rv$loading <- FALSE
          return()
        }

        rv$data    <- df_full
        rv$all_data[[year]] <- df_full
        rv$loading <- FALSE

        # Populate compare player selector from fetched data (all players, no PA filter)
        player_choices <- sort(df_full$player_name[!is.na(df_full$player_name)])
        updateSelectizeInput(session, "compare_players",
                             choices = player_choices, selected = character(0),
                             server  = TRUE)
      })
    })

    # Reactive filtered data (apply min PA after fetch)
    filtered <- reactive({
      df <- rv$data
      if (is.null(df)) return(NULL)
      min_pa <- input$min_pa
      df[!is.na(df$pa) & df$pa >= min_pa, , drop = FALSE]
    })

    # Debounced search term (300 ms) — applied on top of min-PA filter
    search_d <- debounce(reactive(input$search), 300)

    filtered_search <- reactive({
      df <- filtered()
      if (is.null(df)) return(NULL)
      q  <- trimws(search_d() %||% "")
      if (!nzchar(q)) return(df)
      q_lower <- tolower(q)
      keep <- grepl(q_lower, tolower(df$player_name), fixed = TRUE) |
              grepl(q_lower, tolower(df$team_abbrev),  fixed = TRUE)
      df[keep, , drop = FALSE]
    })

    # Stat selector: only render after data is loaded
    output$stat_selector_ui <- renderUI({
      if (is.null(rv$data)) return(NULL)

      card_css <- c(luck = "hd-selector-card-luck",
                    power = "hd-selector-card-power",
                    plate = "hd-selector-card-plate")

      div(
        class = "hd-selector-row",
        lapply(names(HD_STAT_GROUPS), function(grp_name) {
          grp     <- HD_STAT_GROUPS[[grp_name]]
          choices <- setNames(grp$ids, HD_COLS$label[match(grp$ids, HD_COLS$id)])
          div(
            class = paste("hd-selector-card", card_css[[grp_name]]),
            div(class = "hd-selector-card-title", grp$label),
            checkboxGroupInput(
              ns(paste0("show_", grp_name)),
              label    = NULL,
              choices  = choices,
              selected = setdiff(grp$ids, HD_DEFAULT_UNCHECKED),
              inline   = FALSE
            )
          )
        })
      )
    })

    # Reactive: combined selected stat columns
    # Fallbacks match the checkbox defaults (HD_DEFAULT_UNCHECKED excluded)
    selected_stats <- reactive({
      lk <- if (!is.null(input$show_luck))  input$show_luck  else setdiff(HD_STAT_GROUPS$luck$ids,  HD_DEFAULT_UNCHECKED)
      pw <- if (!is.null(input$show_power)) input$show_power else setdiff(HD_STAT_GROUPS$power$ids, HD_DEFAULT_UNCHECKED)
      pl <- if (!is.null(input$show_plate)) input$show_plate else setdiff(HD_STAT_GROUPS$plate$ids, HD_DEFAULT_UNCHECKED)
      c(lk, pw, pl)
    })

    output$fetch_status <- renderUI({
      if (rv$loading) {
        tags$span(class = "text-muted", "⏳ Loading…")
      } else if (!is.null(rv$error)) {
        tags$span(class = "text-danger", rv$error)
      } else if (!is.null(rv$data)) {
        n_full   <- nrow(rv$data)
        n_filter <- nrow(filtered())
        n_search <- nrow(filtered_search())
        q        <- trimws(input$search %||% "")
        msg <- if (nzchar(q)) {
          sprintf("✓ %d players loaded · %d shown at ≥%d PA · %d match “%s”",
                  n_full, n_filter, input$min_pa, n_search, q)
        } else {
          sprintf("✓ %d players loaded · %d shown at ≥%d PA",
                  n_full, n_filter, input$min_pa)
        }
        tags$span(class = "text-success", msg)
      }
    })

    output$table_ui <- renderUI({
      if (rv$loading) return(div(class = "sps-empty",
        p(tags$span(class = "spinner-border spinner-border-sm me-2", role = "status"),
          "Fetching data…")))
      if (!is.null(rv$error)) return(div(class = "sps-empty",
        p(class = "text-danger", rv$error)))
      if (is.null(rv$data)) return(div(class = "sps-empty",
        p("Click 'Fetch Hitter Data' to load stats.")))
      DT::dataTableOutput(ns("dt"))
    })

    output$dt <- DT::renderDataTable({
      df <- filtered_search()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      la <- compute_league_avg(filtered())   # LA always from full min-PA pool
      render_hit_dashboard_dt(df, la, selected_cols = selected_stats())
    }, server = TRUE)

    # ── Compare tab ──────────────────────────────────────────────────────────

    output$compare_ui <- renderUI({
      if (is.null(rv$data)) {
        return(div(class = "sps-empty",
                   p("Fetch data first, then search for players above.")))
      }
      DT::dataTableOutput(ns("compare_dt"))
    })

    output$compare_dt <- DT::renderDataTable({
      df  <- rv$data
      sel <- input$compare_players
      if (is.null(df) || length(sel) == 0) return(NULL)
      sel_df <- df[df$player_name %in% sel, , drop = FALSE]
      if (nrow(sel_df) == 0) return(NULL)
      render_hit_dashboard_compare_dt(sel_df, selected_cols = selected_stats())
    }, server = FALSE)

    # ── Player Trends tab ────────────────────────────────────────────────────

    # Populate trend player dropdowns from all fetched seasons
    observe({
      all <- rv$all_data
      if (length(all) == 0) return()
      players <- sort(unique(unlist(lapply(all, function(d) d$player_name[!is.na(d$player_name)]))))
      player_choices <- data.frame(
        value  = players,
        label  = players,
        search = iconv(players, to = "ASCII//TRANSLIT", sub = ""),
        stringsAsFactors = FALSE
      )
      so <- list(
        placeholder = "Type a player name...",
        valueField = "value", labelField = "label",
        searchField = c("label", "search"),
        render = I("{option: function(item, escape) {return '<div>' + escape(item.label) + '</div>'}}")
      )
      updateSelectizeInput(session, "trend_player1",
                           choices = player_choices, selected = input$trend_player1 %||% "",
                           server = TRUE, options = so)
      so2 <- so
      so2$placeholder <- "(optional)"
      updateSelectizeInput(session, "trend_player2",
                           choices = player_choices, selected = input$trend_player2 %||% "",
                           server = TRUE, options = so2)
    })

    # Selected metrics for trends
    trend_metrics <- reactive({
      c(input$trend_luck %||% character(0),
        input$trend_power %||% character(0),
        input$trend_plate %||% character(0))
    })

    # Build combined trend data for selected players across all fetched seasons
    trend_data <- reactive({
      all <- rv$all_data
      if (length(all) == 0) return(NULL)

      p1 <- input$trend_player1
      p2 <- input$trend_player2
      players <- c(p1, p2)
      players <- players[!is.na(players) & nchar(players) > 0]
      if (length(players) == 0) return(NULL)

      rows <- lapply(names(all), function(szn) {
        d <- all[[szn]]
        d_sel <- d[d$player_name %in% players, , drop = FALSE]
        if (nrow(d_sel) == 0) return(NULL)
        d_sel$season <- as.integer(szn)
        d_sel
      })
      dplyr::bind_rows(rows)
    })

    # Dynamic chart height
    trend_chart_height <- reactive({
      p2 <- input$trend_player2
      if (!is.null(p2) && nchar(p2) > 0) 720 else 400
    })

    output$trend_chart_ui <- renderUI({
      uiOutput_args <- list()
      if (length(rv$all_data) == 0) {
        return(div(style = "text-align:center;padding:40px;color:#888;",
                   p("Fetch hitter data for one or more seasons, then select a player.")))
      }
      plotOutput(ns("trend_chart"), height = paste0(trend_chart_height(), "px"))
    })

    output$trend_chart <- renderPlot({
      d       <- trend_data()
      metrics <- trend_metrics()

      if (is.null(d) || length(metrics) == 0) {
        plot.new()
        text(0.5, 0.5, "Select a player and at least one metric", cex = 1.2, col = "#888")
        return()
      }

      # Build metric labels and palette
      metric_labels <- setNames(HD_COLS$label[match(metrics, HD_COLS$id)], metrics)
      grp_palettes <- list(
        luck  = c("#2e6da4", "#1a4a7a", "#4a8dc4", "#0d3b6e", "#6aadd4", "#3c7fb5", "#5599cc"),
        power = c("#2a7a4b", "#1d5a36", "#4a9a6b", "#0e4028", "#6aba8b", "#3c8b5c", "#58a878", "#7cc09a", "#3d9060"),
        plate = c("#7a3b6e", "#5a2b4e", "#9a5b8e", "#4a1b3e", "#ba7bae", "#6c4d62", "#8b4d7e")
      )

      pal <- character(0)
      for (m in metrics) {
        for (g in names(HD_STAT_GROUPS)) {
          if (m %in% HD_STAT_GROUPS[[g]]$ids) {
            idx_in_grp <- which(HD_STAT_GROUPS[[g]]$ids == m)
            pal[m] <- grp_palettes[[g]][((idx_in_grp - 1) %% length(grp_palettes[[g]])) + 1]
            break
          }
        }
      }

      # Reshape to long
      long <- d |>
        dplyr::select(season, player_name, dplyr::all_of(metrics)) |>
        tidyr::pivot_longer(cols = -c(season, player_name),
                            names_to = "metric", values_to = "value") |>
        dplyr::filter(!is.na(value)) |>
        dplyr::mutate(
          metric_label = factor(metric_labels[metric], levels = metric_labels[metrics])
        )

      if (nrow(long) == 0) {
        plot.new()
        text(0.5, 0.5, "No data for selected player(s)/metrics", cex = 1.2, col = "#888")
        return()
      }

      players <- unique(d$player_name[!is.na(d$pa)])
      n_players <- length(players)
      fetched_seasons <- sort(unique(d$season))

      # Dual y-axes: if exactly 2 metrics selected with different scale types
      use_dual <- length(metrics) == 2
      if (use_dual) {
        is_pct <- HD_COLS$pct[match(metrics, HD_COLS$id)]
        use_dual <- is_pct[1] != is_pct[2]
      }

      if (use_dual && n_players <= 1) {
        m1 <- metrics[1]; m2 <- metrics[2]
        l1 <- metric_labels[m1]; l2 <- metric_labels[m2]
        d1 <- long[long$metric == m1, ]
        d2 <- long[long$metric == m2, ]
        rng1 <- range(d1$value, na.rm = TRUE)
        rng2 <- range(d2$value, na.rm = TRUE)
        pad1 <- diff(rng1) * 0.15; pad2 <- diff(rng2) * 0.15
        if (pad1 == 0) pad1 <- 1; if (pad2 == 0) pad2 <- 1
        rng1 <- c(rng1[1] - pad1, rng1[2] + pad1)
        rng2 <- c(rng2[1] - pad2, rng2[2] + pad2)

        par(mar = c(5, 4.5, 3, 4.5), bg = "white")
        plot(d1$season, d1$value, type = "b", pch = 19, lwd = 2.5, cex = 1.5,
             col = pal[m1], xlab = "Season", ylab = l1,
             xlim = range(fetched_seasons), ylim = rng1,
             xaxt = "n", main = if (n_players == 1) paste0(players[1], ": Stat Trends") else "")
        axis(1, at = fetched_seasons)
        text(d1$season, d1$value, labels = sprintf("%.1f", d1$value),
             pos = 3, cex = 0.85, col = pal[m1], font = 2)

        par(new = TRUE)
        plot(d2$season, d2$value, type = "b", pch = 17, lwd = 2.5, cex = 1.5,
             col = pal[m2], axes = FALSE, xlab = "", ylab = "",
             xlim = range(fetched_seasons), ylim = rng2)
        axis(4, col.axis = pal[m2])
        mtext(l2, side = 4, line = 3, col = pal[m2], font = 2, cex = 0.9)
        text(d2$season, d2$value, labels = sprintf("%.1f", d2$value),
             pos = 1, cex = 0.85, col = pal[m2], font = 2)

        legend("topright", legend = c(l1, l2), col = c(pal[m1], pal[m2]),
               lwd = 2.5, pch = c(19, 17), bg = "white", cex = 0.9)
        grid(col = "#ddd", lty = 1, lwd = 0.3)
      } else {
        # Standard ggplot: single y-axis, facet by player if 2 selected
        p <- ggplot2::ggplot(long, ggplot2::aes(x = season, y = value,
                                                 colour = metric_label,
                                                 group = metric_label)) +
          ggplot2::geom_line(linewidth = 1.3) +
          ggplot2::geom_point(size = 3.5) +
          ggrepel::geom_text_repel(
            ggplot2::aes(label = sprintf("%.1f", value)),
            size = 3.3, show.legend = FALSE,
            nudge_y = 0.4, segment.color = "#ccc", segment.size = 0.3,
            min.segment.length = 0.2, box.padding = 0.3,
            direction = "y", seed = 42
          ) +
          ggplot2::scale_colour_manual(
            values = setNames(pal[metrics], metric_labels[metrics]),
            name = NULL
          ) +
          ggplot2::scale_x_continuous(breaks = fetched_seasons, labels = fetched_seasons) +
          ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0.05, 0.15))) +
          ggplot2::labs(x = "Season", y = "") +
          ggplot2::theme_minimal(base_size = 14) +
          ggplot2::theme(
            plot.background  = ggplot2::element_rect(fill = "white", colour = NA),
            panel.background = ggplot2::element_rect(fill = "white", colour = NA),
            panel.grid.major = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
            panel.grid.minor = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
            axis.title    = ggplot2::element_text(size = 12, face = "bold"),
            axis.text     = ggplot2::element_text(size = 10),
            axis.text.x   = ggplot2::element_text(angle = 45, hjust = 1),
            strip.text    = ggplot2::element_text(face = "bold", size = 13),
            plot.margin   = ggplot2::margin(8, 18, 8, 12),
            legend.position = "top"
          )

        if (n_players > 1) {
          long$player_name <- factor(long$player_name, levels = players)
          p <- p + ggplot2::facet_wrap(~ player_name, ncol = 1, scales = "free_y")
        } else if (n_players == 1) {
          p <- p + ggplot2::ggtitle(paste0(players[1], ": Stat Trends"))
        }

        p
      }
    }, res = 96, bg = "white")

    # Trend detail table (wrapped in renderUI to avoid DT init on empty div)
    output$trend_table_ui <- renderUI({
      d <- trend_data()
      if (is.null(d)) return(NULL)
      DT::DTOutput(ns("trend_table"), width = "100%")
    })

    output$trend_table <- DT::renderDT({
      d <- trend_data()
      if (is.null(d)) return(NULL)

      metrics <- trend_metrics()
      show_ids <- c("player_name", "season", "team_abbrev", "pa", metrics)
      show_ids <- intersect(show_ids, names(d))

      tbl <- d[, show_ids, drop = FALSE]
      # Rename columns to labels
      col_labels <- HD_COLS$label[match(names(tbl), HD_COLS$id)]
      col_labels[is.na(col_labels)] <- names(tbl)[is.na(col_labels)]
      col_labels[names(tbl) == "season"] <- "Season"
      names(tbl) <- col_labels

      # Round numeric columns
      for (i in seq_along(tbl)) {
        if (is.numeric(tbl[[i]])) tbl[[i]] <- round(tbl[[i]], 1)
      }

      tbl <- tbl[order(tbl$Player, tbl$Season), , drop = FALSE]

      DT::datatable(
        tbl, rownames = FALSE, selection = "none",
        options = list(dom = "t", paging = FALSE, ordering = FALSE,
                       columnDefs = list(
                         list(className = "dt-center", targets = "_all"),
                         list(className = "dt-left", targets = 0)
                       )),
        class = "compact stripe"
      )
    })
  })
}
