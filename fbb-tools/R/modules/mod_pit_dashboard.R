suppressPackageStartupMessages({ library(DT) })

# ── Constants ──────────────────────────────────────────────────────────────────

PD_SV_CUSTOM    <- "https://baseballsavant.mlb.com/leaderboard/custom"
PD_FG_BASE      <- "https://www.fangraphs.com/api/leaders/major-league/data"
PD_FG_UA        <- paste0(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# Savant custom leaderboard selections for pitchers
PD_SV_SELECTIONS <- paste0(
  "k_percent,bb_percent,whiff_percent,xera,",
  "p_ball_percent,p_strike_percent"
)

PD_SEASONS        <- c("2024", "2025", "2026")
PD_DEFAULT_MIN_IP <- 10L

# Column order matches display grouping:
#   Identity | Strikeouts | Command | ERA Estimators | Luck Stats | Pitch Modeling
PD_COLS <- data.frame(
  id    = c(
    "player_name", "team_abbrev", "ip",
    # Strikeouts
    "k_pct", "k_per_9", "swstr_pct", "whiff_pct", "csw_pct",
    "contact_pct", "z_con_pct", "o_con_pct", "o_swing_pct",
    # Command
    "strike_pct", "ball_pct", "bb_pct", "bb_per_9",
    # ERA Estimators
    "era", "siera", "xfip", "xera", "k_minus_bb_pct",
    # Luck Stats
    "babip", "lob_pct", "hr_fb_pct",
    # Pitch Modeling
    "stuff_plus", "loc_plus", "pitching_plus",
    "bot_stf", "bot_cmd", "bot_ovr"
  ),
  label = c(
    "Player", "Team", "IP",
    "K%", "K/9", "SwStr%", "Whiff%", "CSW%",
    "Contact%", "Z-Con%", "O-Con%", "O-Swing%",
    "Strike%", "Ball%", "BB%", "BB/9",
    "ERA", "SIERA", "xFIP", "xERA", "K-BB%",
    "BABIP", "LOB%", "HR/FB%",
    "Stuff+", "Loc+", "Pitching+",
    "botStf", "botCmd", "botOvr"
  ),
  dir   = c(
     0,  0,  0,
    # Strikeouts: K% up=good, K/9 up=good, SwStr% up=good, Whiff% up=good,
    # CSW% up=good, Contact% down=good, Z-Con% down=good, O-Con% down=good, O-Swing% up=good
     1,  1,  1,  1,  1,
    -1, -1, -1,  1,
    # Command: Strike% up=good, Ball% down=good, BB% down=good, BB/9 down=good
     1, -1, -1, -1,
    # ERA Estimators: ERA down=good, SIERA down=good, xFIP down=good, xERA down=good, K-BB% up=good
    -1, -1, -1, -1,  1,
    # Luck Stats: BABIP no dir (context), LOB% no dir, HR/FB% no dir
     0,  0,  0,
    # Pitch Modeling: all up=good
     1,  1,  1,  1,  1,  1
  ),
  pct   = c(
    FALSE, FALSE, FALSE,
    # Strikeouts
    TRUE, FALSE, TRUE, TRUE, TRUE,
    TRUE, TRUE, TRUE, TRUE,
    # Command
    TRUE, TRUE, TRUE, FALSE,
    # ERA Estimators
    FALSE, FALSE, FALSE, FALSE, TRUE,
    # Luck Stats
    FALSE, TRUE, TRUE,
    # Pitch Modeling
    FALSE, FALSE, FALSE, FALSE, FALSE, FALSE
  ),
  # Format type: "pct" = XX.X%, "era" = X.XX, "int" = integer, "dec1" = X.X, "dec0" = whole number
  fmt = c(
    "text", "text", "dec1",
    # Strikeouts
    "pct", "dec2", "pct", "pct", "pct",
    "pct", "pct", "pct", "pct",
    # Command
    "pct", "pct", "pct", "dec2",
    # ERA Estimators
    "era", "era", "era", "era", "pct",
    # Luck Stats
    "era", "pct", "pct",
    # Pitch Modeling
    "int", "int", "int", "int", "int", "int"
  ),
  stringsAsFactors = FALSE
)

# Column IDs always shown (never in stat selector)
PD_ID_COLS <- c("player_name", "team_abbrev", "ip")

# Stat columns unchecked by default in the selector
# (User specified * = display by default; everything without * is unchecked)
PD_DEFAULT_UNCHECKED <- c(
  # Strikeouts: K/9, CSW%, Contact%, Z-Con%, O-Con%, O-Swing%
  "k_per_9", "csw_pct", "contact_pct", "z_con_pct", "o_con_pct", "o_swing_pct",
  # Command: Strike%, Ball%, BB/9
  "strike_pct", "ball_pct", "bb_per_9",
  # ERA Estimators: xERA
  "xera",
  # Luck Stats: (all checked by default)
  # Pitch Modeling: Loc+, botStf, botCmd, botOvr
  "loc_plus", "bot_stf", "bot_cmd", "bot_ovr"
)

# Columns used for IP-weighted league average (exclude identity cols)
PD_RATE_COLS <- PD_COLS$id[!PD_COLS$id %in% PD_ID_COLS]

# Stat groups for selector (in display order)
PD_STAT_GROUPS <- list(
  strikeouts = list(
    label = "Strikeouts",
    ids   = c("k_pct", "k_per_9", "swstr_pct", "whiff_pct", "csw_pct",
              "contact_pct", "z_con_pct", "o_con_pct", "o_swing_pct")
  ),
  command = list(
    label = "Command",
    ids   = c("strike_pct", "ball_pct", "bb_pct", "bb_per_9")
  ),
  era_est = list(
    label = "ERA Estimators",
    ids   = c("era", "siera", "xfip", "xera", "k_minus_bb_pct")
  ),
  luck = list(
    label = "Luck Stats",
    ids   = c("babip", "lob_pct", "hr_fb_pct")
  ),
  pitch_model = list(
    label = "Pitch Modeling",
    ids   = c("stuff_plus", "loc_plus", "pitching_plus",
              "bot_stf", "bot_cmd", "bot_ovr")
  )
)

# Group colors
PD_GRP_COLORS <- list(
  strikeouts  = list(bg = "rgba(178,34,34,0.07)",  fg = "#b22222",  hdr = "#b22222"),
  command     = list(bg = "rgba(46,109,164,0.07)",  fg = "#2e6da4",  hdr = "#2e6da4"),
  era_est     = list(bg = "rgba(42,122,75,0.07)",   fg = "#2a7a4b",  hdr = "#2a7a4b"),
  luck        = list(bg = "rgba(156,120,42,0.07)",   fg = "#9c782a",  hdr = "#9c782a"),
  pitch_model = list(bg = "rgba(122,59,110,0.07)",  fg = "#7a3b6e",  hdr = "#7a3b6e")
)

# ── Data fetchers ──────────────────────────────────────────────────────────────

# Generic Savant CSV fetch — download.file (libcurl, no shell escaping issues)
.pd_sv_read <- function(url) {
  tmp <- tempfile(fileext = ".csv")
  on.exit(unlink(tmp), add = TRUE)

  ok <- tryCatch({
    download.file(url, tmp, quiet = TRUE, method = "libcurl",
                  headers = c("Accept" = "text/csv, */*",
                              "User-Agent" = PD_FG_UA))
    TRUE
  }, error = function(e) {
    tryCatch({ download.file(url, tmp, quiet = TRUE, method = "auto"); TRUE },
             error = function(e2) FALSE)
  })
  if (!ok) return(NULL)

  tryCatch({
    lines <- readLines(tmp, warn = FALSE)
    if (length(lines) == 0) return(NULL)
    lines[1] <- sub("^\xef\xbb\xbf", "", lines[1])
    read.csv(text = paste(lines, collapse = "\n"),
             stringsAsFactors = FALSE, check.names = FALSE, quote = "\"")
  }, error = function(e) NULL)
}

# FanGraphs JSON fetch with curl fallback
.pd_fg_fetch <- function(url) {
  payload <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = TRUE),
    error = function(e) NULL
  )
  if (!is.null(payload)) return(list(ok = TRUE, payload = payload))

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin))
    return(list(ok = FALSE, error = "jsonlite failed and curl not available"))

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  result <- tryCatch(
    system2(curl_bin, args = c(
      "-sS", "-L", "--fail", "--compressed",
      "--max-time", "30", "--connect-timeout", "10",
      "--retry", "2", "--retry-delay", "1",
      "-A", PD_FG_UA,
      "-H", "Accept: application/json, text/plain, */*",
      "-H", "Referer: https://www.fangraphs.com/leaders/major-league",
      url, "-o", tmp
    ), stdout = TRUE, stderr = TRUE),
    error = function(e) NULL
  )
  if (is.null(result)) return(list(ok = FALSE, error = "curl execution failed"))
  status <- attr(result, "status")
  if (!is.null(status) && as.integer(status) != 0L)
    return(list(ok = FALSE, error = paste("curl exit status:", status)))

  payload <- tryCatch(jsonlite::fromJSON(tmp, simplifyVector = TRUE), error = function(e) NULL)
  if (!is.null(payload)) return(list(ok = TRUE, payload = payload))
  list(ok = FALSE, error = "JSON parse failed after curl")
}

# FG JSON → data frame
.pd_fg_parse <- function(result) {
  if (!isTRUE(result$ok)) return(NULL)
  p  <- result$payload
  df <- if (is.data.frame(p)) p else if (is.data.frame(p$data)) p$data else NULL
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}

# Robust case-insensitive column extractors
.pd_col_num <- function(df, ...) {
  cn <- tolower(names(df))
  for (nm in tolower(c(...))) {
    idx <- match(nm, cn)
    if (!is.na(idx)) return(suppressWarnings(as.numeric(df[[idx]])))
  }
  rep(NA_real_, nrow(df))
}

.pd_col_chr <- function(df, ...) {
  cn <- tolower(names(df))
  for (nm in tolower(c(...))) {
    idx <- match(nm, cn)
    if (!is.na(idx)) return(as.character(df[[idx]]))
  }
  rep(NA_character_, nrow(df))
}

.pd_strip_html <- function(x) trimws(gsub("<[^>]+>", "", x))

# ── Source 1: Savant Custom Leaderboard (pitcher) ─────────────────────────────
# Returns: K%, BB%, Whiff%, xERA, Ball%, Strike%

fetch_pd_savant <- function(year) {
  url <- paste0(
    PD_SV_CUSTOM,
    "?year=", year,
    "&type=pitcher&filter=&min=1",
    "&selections=", utils::URLencode(PD_SV_SELECTIONS, reserved = TRUE),
    "&chart=false&csv=true"
  )
  raw <- .pd_sv_read(url)
  if (is.null(raw)) return(NULL)

  name_col <- grep("last_name", names(raw), value = TRUE)[1]
  if (is.na(name_col)) return(NULL)

  parts <- strsplit(raw[[name_col]], ", ", fixed = TRUE)
  raw$player_name <- vapply(parts, function(p) {
    if (length(p) == 2L) paste(trimws(p[2]), trimws(p[1])) else trimws(p[1])
  }, character(1))
  raw$name_key <- player_nk(raw$player_name)

  cn  <- names(raw)
  num <- function(col) {
    if (col %in% cn) suppressWarnings(as.numeric(raw[[col]])) else rep(NA_real_, nrow(raw))
  }

  data.frame(
    player_id   = suppressWarnings(as.integer(raw[["player_id"]])),
    player_name = raw$player_name,
    name_key    = raw$name_key,
    k_pct       = num("k_percent"),
    bb_pct      = num("bb_percent"),
    whiff_pct   = num("whiff_percent"),
    xera        = num("xera"),
    ball_pct    = num("p_ball_percent"),
    strike_pct  = num("p_strike_percent"),
    stringsAsFactors = FALSE
  )
}

# ── Source 2: FG Dashboard (type=1, stats=pit) ────────────────────────────────
# Returns: ERA, SIERA, xFIP, K-BB%, K/9, BB/9, BABIP, LOB%, HR/FB%, IP, GS, G

fetch_pd_fg_dashboard <- function(year) {
  url <- paste0(
    PD_FG_BASE,
    "?pos=all&stats=pit&lg=all&qual=0&ind=0",
    "&team=0&rost=0&players=0",
    "&type=1",
    "&season=", year, "&season1=", year,
    "&pageitems=2000&pagenum=1"
  )
  df <- .pd_fg_parse(.pd_fg_fetch(url))
  if (is.null(df)) return(NULL)

  mlbam_id <- suppressWarnings(as.integer(.pd_col_chr(df, "xMLBAMID", "xmlbamid", "mlbamid")))
  names_raw <- .pd_strip_html(.pd_col_chr(df, "Name", "PlayerName", "name"))
  team_raw  <- .pd_strip_html(.pd_col_chr(df, "Team", "TeamName", "teamname"))

  # FG returns rate stats as 0-1 fractions for K%, BB%, K-BB%, LOB%, HR/FB
  data.frame(
    player_id    = mlbam_id,
    player_name  = names_raw,
    name_key     = player_nk(names_raw),
    team_abbrev  = team_raw,
    ip           = .pd_col_num(df, "IP"),
    gs           = .pd_col_num(df, "GS"),
    g            = .pd_col_num(df, "G"),
    era          = .pd_col_num(df, "ERA"),
    siera        = .pd_col_num(df, "SIERA"),
    xfip         = .pd_col_num(df, "xFIP"),
    k_minus_bb_pct = .pd_col_num(df, "K-BB%") * 100,
    k_per_9      = .pd_col_num(df, "K/9"),
    bb_per_9     = .pd_col_num(df, "BB/9"),
    babip        = .pd_col_num(df, "BABIP"),
    lob_pct      = .pd_col_num(df, "LOB%") * 100,
    hr_fb_pct    = .pd_col_num(df, "HR/FB") * 100,
    stringsAsFactors = FALSE
  )
}

# ── Source 3: FG Plate Discipline (type=5, stats=pit) ─────────────────────────
# Returns: SwStr%, CSW%, Contact%, Z-Contact%, O-Contact%, O-Swing%

fetch_pd_fg_discipline <- function(year) {
  url <- paste0(
    PD_FG_BASE,
    "?pos=all&stats=pit&lg=all&qual=0&ind=0",
    "&team=0&rost=0&players=0",
    "&type=5",
    "&season=", year, "&season1=", year,
    "&pageitems=2000&pagenum=1"
  )
  df <- .pd_fg_parse(.pd_fg_fetch(url))
  if (is.null(df)) return(NULL)

  mlbam_id <- suppressWarnings(as.integer(.pd_col_chr(df, "xMLBAMID", "xmlbamid", "mlbamid")))

  # FG returns plate discipline as 0-1 fractions
  data.frame(
    player_id   = mlbam_id,
    swstr_pct   = .pd_col_num(df, "SwStr%") * 100,
    csw_pct     = .pd_col_num(df, "CSW%") * 100,
    contact_pct = .pd_col_num(df, "Contact%") * 100,
    z_con_pct   = .pd_col_num(df, "Z-Contact%") * 100,
    o_con_pct   = .pd_col_num(df, "O-Contact%") * 100,
    o_swing_pct = .pd_col_num(df, "O-Swing%") * 100,
    stringsAsFactors = FALSE
  )
}

# ── Source 4: FG Stuff+ (custom type, stats=pit) ─────────────────────────────
# Returns: Stuff+, Location+, Pitching+

fetch_pd_fg_stuff <- function(year) {
  url <- paste0(
    PD_FG_BASE,
    "?pos=all&stats=pit&lg=all&qual=0&ind=0",
    "&team=0&rost=0&players=0",
    "&type=c,386,387,388",
    "&season=", year, "&season1=", year,
    "&pageitems=2000&pagenum=1"
  )
  df <- .pd_fg_parse(.pd_fg_fetch(url))
  if (is.null(df)) return(NULL)

  mlbam_id <- suppressWarnings(as.integer(.pd_col_chr(df, "xMLBAMID", "xmlbamid", "mlbamid")))

  data.frame(
    player_id    = mlbam_id,
    stuff_plus   = .pd_col_num(df, "Stuff+", "sp_stuff"),
    loc_plus     = .pd_col_num(df, "Location+", "sp_location"),
    pitching_plus = .pd_col_num(df, "Pitching+", "sp_pitching"),
    stringsAsFactors = FALSE
  )
}

# ── Source 5: FG PitchingBot (type=26, stats=pit) ────────────────────────────
# Returns: botStf, botCmd, botOvr (20-80 scouting scale)

fetch_pd_fg_pitchingbot <- function(year) {
  url <- paste0(
    PD_FG_BASE,
    "?pos=all&stats=pit&lg=all&qual=0&ind=0",
    "&team=0&rost=0&players=0",
    "&type=26",
    "&season=", year, "&season1=", year,
    "&pageitems=2000&pagenum=1"
  )
  df <- .pd_fg_parse(.pd_fg_fetch(url))
  if (is.null(df)) return(NULL)

  mlbam_id <- suppressWarnings(as.integer(.pd_col_chr(df, "xMLBAMID", "xmlbamid", "mlbamid")))

  data.frame(
    player_id = mlbam_id,
    bot_stf   = .pd_col_num(df, "botStf"),
    bot_cmd   = .pd_col_num(df, "botCmd"),
    bot_ovr   = .pd_col_num(df, "botOvr"),
    stringsAsFactors = FALSE
  )
}

# ── Build combined data frame ──────────────────────────────────────────────────

build_pit_dashboard <- function(sv_custom, fg_dash, fg_disc, fg_stuff, fg_bot) {
  if (is.null(fg_dash) || nrow(fg_dash) == 0) return(NULL)

  # Start with FG dashboard as base (has player_id, name, team, IP)
  df <- fg_dash

  # Join Savant data by player_id, fallback to name_key
  if (!is.null(sv_custom) && nrow(sv_custom) > 0) {
    sv <- sv_custom[!duplicated(sv_custom$player_id), ]
    sv_cols <- c("k_pct", "bb_pct", "whiff_pct", "xera", "ball_pct", "strike_pct")

    m <- match(df$player_id, sv$player_id)
    unmatched <- is.na(m)
    if (any(unmatched) && "name_key" %in% names(df) && "name_key" %in% names(sv)) {
      m2 <- match(df$name_key[unmatched], sv$name_key)
      m[unmatched] <- ifelse(!is.na(m2), which(!duplicated(sv$name_key))[m2], NA_integer_)
    }
    for (col in sv_cols) {
      if (col %in% names(sv)) df[[col]] <- sv[[col]][m]
    }
  }

  # Join FG plate discipline by player_id
  if (!is.null(fg_disc) && nrow(fg_disc) > 0) {
    disc <- fg_disc[!duplicated(fg_disc$player_id), ]
    disc_cols <- c("swstr_pct", "csw_pct", "contact_pct", "z_con_pct", "o_con_pct", "o_swing_pct")
    m <- match(df$player_id, disc$player_id)
    for (col in disc_cols) {
      if (col %in% names(disc)) df[[col]] <- disc[[col]][m]
    }
  }

  # Join FG Stuff+ by player_id
  if (!is.null(fg_stuff) && nrow(fg_stuff) > 0) {
    stf <- fg_stuff[!duplicated(fg_stuff$player_id), ]
    stf_cols <- c("stuff_plus", "loc_plus", "pitching_plus")
    m <- match(df$player_id, stf$player_id)
    for (col in stf_cols) {
      if (col %in% names(stf)) df[[col]] <- stf[[col]][m]
    }
  }

  # Join FG PitchingBot by player_id
  if (!is.null(fg_bot) && nrow(fg_bot) > 0) {
    bot <- fg_bot[!duplicated(fg_bot$player_id), ]
    bot_cols <- c("bot_stf", "bot_cmd", "bot_ovr")
    m <- match(df$player_id, bot$player_id)
    for (col in bot_cols) {
      if (col %in% names(bot)) df[[col]] <- bot[[col]][m]
    }
  }

  # Compute K-BB% from Savant K% and BB% if FG version is missing
  if (!"k_minus_bb_pct" %in% names(df) || all(is.na(df$k_minus_bb_pct))) {
    if ("k_pct" %in% names(df) && "bb_pct" %in% names(df)) {
      df$k_minus_bb_pct <- df$k_pct - df$bb_pct
    }
  }

  # Ensure all PD_COLS columns exist
  for (col in PD_COLS$id) {
    if (!col %in% names(df)) df[[col]] <- NA_real_
  }

  df
}

# ── League average ─────────────────────────────────────────────────────────────

compute_pit_league_avg <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  ip <- df$ip
  ip[is.na(ip) | ip <= 0] <- 0

  la <- data.frame(
    player_name = "── League Avg ──",
    team_abbrev = "",
    stringsAsFactors = FALSE
  )
  la$ip <- round(mean(ip[ip > 0], na.rm = TRUE), 1)

  for (col in PD_RATE_COLS) {
    if (!col %in% names(df)) { la[[col]] <- NA_real_; next }
    vals  <- suppressWarnings(as.numeric(df[[col]]))
    valid <- !is.na(vals) & ip > 0
    if (!any(valid)) { la[[col]] <- NA_real_; next }
    la[[col]] <- stats::weighted.mean(vals[valid], ip[valid], na.rm = TRUE)
  }
  la
}

# ── DT rendering ──────────────────────────────────────────────────────────────

render_pit_dashboard_dt <- function(df, la = NULL, compare = FALSE, selected_cols = NULL) {
  if (is.null(df) || nrow(df) == 0)
    return(DT::datatable(data.frame(Message = "No data available.")))

  # Resolve which columns to show (identity cols always included)
  if (!is.null(selected_cols)) {
    keep_ids  <- c(PD_ID_COLS, intersect(PD_COLS$id, selected_cols))
    show_cols <- PD_COLS[PD_COLS$id %in% keep_ids, , drop = FALSE]
    show_cols <- show_cols[order(match(show_cols$id, PD_COLS$id)), , drop = FALSE]
  } else {
    show_cols <- PD_COLS
  }

  # Subset df to shown columns
  df_show <- df[, show_cols$id, drop = FALSE]

  n_show  <- nrow(show_cols)
  col_idx <- setNames(seq_len(n_show) - 1L, show_cols$id)
  ip_col  <- col_idx[["ip"]]
  kbb_col <- col_idx[["k_minus_bb_pct"]]

  # .row_order column (hidden; drives LA pinning)
  row_order_ci <- n_show

  # Build combined data: LA row first (row_order=0), then players (row_order=1..n)
  if (!compare && !is.null(la) && nrow(la) > 0) {
    la_show            <- la[1, show_cols$id, drop = FALSE]
    la_show$.row_order <- 0L
    df_plot            <- df_show
    df_plot$.row_order <- seq_len(nrow(df_show))
    dt_data <- rbind(la_show, df_plot)
  } else {
    df_plot            <- df_show
    df_plot$.row_order <- seq_len(nrow(df_show))
    dt_data <- df_plot
  }

  # Group boundary cols — first col of each stat group gets a left border
  grp_starts <- intersect(
    c("k_pct", "strike_pct", "era", "babip", "stuff_plus"),
    show_cols$id
  )

  # ── JS render functions ────────────────────────────────────────────────────
  r_pct <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(1)+'%';
  }")
  r_era <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(2);
  }")
  r_dec2 <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(2);
  }")
  r_dec1 <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return(+d).toFixed(1);
  }")
  r_int <- DT::JS("function(d,t,r){
    if(t!=='display')return+d;
    if(d==null||d===''||isNaN(+d))return '';
    return Math.round(+d).toString();
  }")

  # ── Per-column defs with explicit width control ────────────────────────────
  col_defs <- lapply(seq_len(n_show), function(i) {
    col <- show_cols$id[i]
    fmt <- show_cols$fmt[i]
    ci  <- col_idx[[col]]
    aln <- if (col == "player_name") "dt-left" else "dt-center"
    bdr <- if (col %in% grp_starts)  "pd-grp-start" else ""
    cls <- trimws(paste(aln, bdr))

    # Column widths: force alignment between header and data
    w <- if (col == "player_name") "140px"
         else if (col == "team_abbrev") "45px"
         else if (col == "ip") "45px"
         else "62px"

    if (col %in% c("player_name", "team_abbrev")) {
      return(list(targets = ci, className = cls, width = w))
    }
    rnd <- switch(fmt,
      pct  = r_pct,
      era  = r_era,
      dec2 = r_dec2,
      dec1 = r_dec1,
      int  = r_int,
      r_dec1  # fallback
    )
    list(targets = ci, className = cls, type = "num", render = rnd, width = w)
  })

  # Hide .row_order column
  col_defs <- c(col_defs, list(list(targets = row_order_ci, visible = FALSE)))

  # ── Row callback: style the LA row ─────────────────────────────────────────
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
  stat_ids <- show_cols$id[!show_cols$id %in% PD_ID_COLS]
  grp_ns   <- vapply(names(PD_STAT_GROUPS), function(g) {
    sum(stat_ids %in% PD_STAT_GROUPS[[g]]$ids)
  }, integer(1))

  # ── Column label headers with group coloring ───────────────────────────────
  stat_to_grp <- function(col) {
    for (g in names(PD_STAT_GROUPS)) {
      if (col %in% PD_STAT_GROUPS[[g]]$ids) return(g)
    }
    NA_character_
  }

  mk_col_th <- function(i) {
    col <- show_cols$id[i]
    lbl <- show_cols$label[i]
    grp <- stat_to_grp(col)
    bg  <- if (!is.na(grp)) paste0("background:", PD_GRP_COLORS[[grp]]$bg, ";") else ""
    bdr <- if (col %in% grp_starts) "border-left:3px solid rgba(100,100,100,0.2);" else ""
    tags$th(lbl, style = paste0(bg, bdr))
  }

  # ── Build group header row (row 1 of thead) ────────────────────────────────
  base_sty <- paste0(
    "text-align:center;font-size:0.72rem;font-weight:700;",
    "letter-spacing:.05em;text-transform:uppercase;color:#fff;padding:5px 6px;"
  )
  sep_sty <- "border-left:3px solid rgba(255,255,255,0.35);"

  grp_order <- c("strikeouts", "command", "era_est", "luck", "pitch_model")
  grp_ths <- list(
    # Identity columns spacer
    tags$th(colspan = length(PD_ID_COLS),
            style = "border:none;background:transparent;")
  )
  first_stat <- TRUE
  for (g in grp_order) {
    n <- grp_ns[[g]]
    if (n == 0) next
    sep <- if (!first_stat) sep_sty else ""
    first_stat <- FALSE
    grp_ths <- c(grp_ths, list(
      tags$th(PD_STAT_GROUPS[[g]]$label,
              colspan = n,
              style   = paste0(base_sty, sep, "background:", PD_GRP_COLORS[[g]]$hdr, ";"))
    ))
  }
  # Hidden .row_order column spacer (must match total column count)
  grp_ths <- c(grp_ths, list(tags$th(style = "display:none;", "")))

  # ── Two-row container: group row + column name row ─────────────────────────
  grp_tr <- do.call(tags$tr, c(list(class = "pd-grp-hdr"), grp_ths))
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
      pageLength     = nrow(df_show),
      dom            = "t",
      ordering       = FALSE,
      scrollX        = TRUE,
      autoWidth      = FALSE,
      columnDefs     = col_defs
    )
  } else {
    # initComplete: disable sorting on group header row, ensure column name row sorts
    init_cb <- DT::JS(
      "function(settings, json) {",
      "  var $thead = $(this.api().table().header());",
      "  $thead.find('tr.pd-grp-hdr th').off('click.DT').css('cursor','default');",
      "}"
    )
    opts <- list(
      pageLength     = 50,
      lengthMenu     = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
      scrollX        = TRUE,
      autoWidth      = FALSE,
      orderCellsTop  = FALSE,
      dom            = "lrtip",
      order          = list(list(kbb_col, "desc")),
      orderFixed     = list(list(row_order_ci, "asc")),
      columnDefs     = col_defs,
      rowCallback    = la_row_cb,
      initComplete   = init_cb
    )
  }

  DT::datatable(
    dt_data,
    rownames   = FALSE,
    container  = container,
    class      = "pd-dt display compact nowrap",
    filter     = "none",
    extensions = character(0),
    options    = opts
  )
}

# ── Compare table: transposed (stats = rows, players = columns) ───────────────

render_pit_dashboard_compare_dt <- function(sel_df, selected_cols = NULL) {
  if (is.null(sel_df) || nrow(sel_df) == 0)
    return(DT::datatable(data.frame(Message = "No data available.")))

  if (!is.null(selected_cols)) {
    keep_ids  <- c(PD_ID_COLS, intersect(PD_COLS$id, selected_cols))
    show_cols <- PD_COLS[PD_COLS$id %in% keep_ids, , drop = FALSE]
    show_cols <- show_cols[order(match(show_cols$id, PD_COLS$id)), , drop = FALSE]
  } else {
    show_cols <- PD_COLS
  }

  stat_cols <- show_cols[!show_cols$id %in% c("player_name", "team_abbrev"), , drop = FALSE]
  players   <- sel_df$player_name

  fmt_val <- function(x, col_id) {
    if (is.na(x) || !is.finite(x)) return("")
    f <- PD_COLS$fmt[PD_COLS$id == col_id]
    switch(f,
      pct  = sprintf("%.1f%%", x),
      era  = sprintf("%.2f", x),
      dec2 = sprintf("%.2f", x),
      dec1 = sprintf("%.1f", x),
      int  = as.character(round(x)),
      sprintf("%.1f", x)
    )
  }

  out_rows <- lapply(seq_len(nrow(stat_cols)), function(i) {
    col_id    <- stat_cols$id[i]
    lbl       <- stat_cols$label[i]
    direction <- stat_cols$dir[i]
    vals <- vapply(players, function(pname) {
      v <- sel_df[sel_df$player_name == pname, col_id, drop = TRUE]
      if (length(v) == 0) NA_real_ else suppressWarnings(as.numeric(v[1]))
    }, numeric(1))
    formatted <- vapply(vals, fmt_val, character(1), col_id = col_id)

    # Highlight best value(s) when direction is known and >1 player
    if (direction != 0 && sum(!is.na(vals)) > 1) {
      best_val <- if (direction > 0) max(vals, na.rm = TRUE) else min(vals, na.rm = TRUE)
      f <- PD_COLS$fmt[PD_COLS$id == col_id]
      tol <- switch(f, pct = 0.05, era = 0.005, dec2 = 0.005, int = 0.5, 0.05)
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
  grp_labels <- lapply(names(PD_STAT_GROUPS), function(g) {
    PD_COLS$label[PD_COLS$id %in% PD_STAT_GROUPS[[g]]$ids]
  })
  names(grp_labels) <- names(PD_STAT_GROUPS)

  row_cb <- DT::JS(sprintf(
    "function(row, data, index) {
      var stat = data[0];
      var groups = %s;
      var colors = %s;
      for (var g in groups) {
        if (groups[g].indexOf(stat) >= 0) {
          $(row).css('background-color', colors[g]);
          return;
        }
      }
    }",
    jsonlite::toJSON(grp_labels, auto_unbox = FALSE),
    jsonlite::toJSON(
      setNames(
        lapply(names(PD_GRP_COLORS), function(g) PD_GRP_COLORS[[g]]$bg),
        names(PD_GRP_COLORS)
      ),
      auto_unbox = TRUE
    )
  ))

  DT::datatable(
    transposed,
    rownames   = FALSE,
    escape     = FALSE,
    class      = "pd-dt display compact",
    extensions = character(0),
    options    = list(
      pageLength  = nrow(transposed),
      dom         = "t",
      ordering    = FALSE,
      scrollX     = TRUE,
      rowCallback = row_cb,
      columnDefs  = list(
        list(targets = 0,      className = "dt-left", width = "100px"),
        list(targets = "_all", className = "dt-center")
      )
    )
  )
}

# ── Shiny UI ──────────────────────────────────────────────────────────────────

pitDashUI <- function(id) {
  ns <- NS(id)

  div(
    class = "pd-page",
    tags$style(HTML("
      .pd-grp-start { border-left: 3px solid rgba(80,80,80,0.18) !important; }

      /* Force column width alignment between header and body */
      .pd-dt table.dataTable th,
      .pd-dt table.dataTable td {
        box-sizing: border-box;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      /* Stat selector: cards */
      .pd-selector-row {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        margin: 14px 0 6px;
      }
      .pd-selector-card {
        flex: 1 1 160px;
        background: var(--card, #fff);
        border: 1px solid var(--line, #dee2e6);
        border-radius: var(--r-md, 6px);
        padding: 12px 16px 10px;
      }
      .pd-selector-card-title {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 8px;
        padding-bottom: 6px;
        border-bottom: 1px solid var(--line, #dee2e6);
      }
      .pd-selector-card-strikeouts  .pd-selector-card-title { color: #b22222; }
      .pd-selector-card-command     .pd-selector-card-title { color: #2e6da4; }
      .pd-selector-card-era_est     .pd-selector-card-title { color: #2a7a4b; }
      .pd-selector-card-luck        .pd-selector-card-title { color: #9c782a; }
      .pd-selector-card-pitch_model .pd-selector-card-title { color: #7a3b6e; }
      .pd-selector-card .shiny-input-container { margin-bottom: 0; }
      .pd-selector-card .checkbox-inline {
        display: block;
        margin: 0 0 4px 0;
        font-size: 0.82rem;
        color: var(--text, #212529);
      }
      .pd-selector-card .checkbox-inline input[type=checkbox] { margin-right: 5px; }
    ")),

    # Page header
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "In-Season Tools"),
      h1(class = "pf-title", "Pitcher Dashboard"),
      p(class = "pf-subtitle",
        "Strikeouts, command, ERA estimators, luck indicators, and pitch modeling for all pitchers.",
        tags$br(),
        tags$span(class = "text-muted",
          "Sources: ",
          tags$b("Statcast"), " · ",
          tags$b("FanGraphs"), " · ",
          tags$b("PitchingBot")
        )
      )
    ),

    # Season + Min IP controls
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season"),
        div(class = "pf-toggle",
            radioButtons(ns("season"), label = NULL,
                         choices  = setNames(PD_SEASONS, PD_SEASONS),
                         selected = PD_SEASONS[length(PD_SEASONS)],
                         inline   = TRUE))
      ),
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Min IP"),
        numericInput(ns("min_ip"), label = NULL,
                     value = PD_DEFAULT_MIN_IP, min = 1, max = 9999, step = 1,
                     width = "80px")
      )
    ),

    # Fetch row
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch Pitcher Data",
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
                placeholder = "Search for pitchers to compare…",
                maxItems    = 8,
                plugins     = list("remove_button")
              )
            )
          ),
          uiOutput(ns("compare_ui"))
        )
      )
    )
  )
}

# ── Shiny server ──────────────────────────────────────────────────────────────

pitDashServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      data    = NULL,
      error   = NULL,
      loading = FALSE
    )

    observeEvent(input$fetch, {
      rv$loading <- TRUE
      rv$error   <- NULL
      rv$data    <- NULL

      year <- input$season

      withProgress(message = "Fetching pitcher data…", value = 0, {

        setProgress(0.05, detail = "FanGraphs dashboard stats…")
        fg_dash <- tryCatch(fetch_pd_fg_dashboard(year), error = function(e) {
          rv$error <- paste("FG dashboard fetch failed:", conditionMessage(e))
          NULL
        })

        if (is.null(fg_dash)) {
          rv$loading <- FALSE
          return()
        }

        setProgress(0.20, detail = "Savant custom leaderboard…")
        sv_custom <- tryCatch(fetch_pd_savant(year), error = function(e) NULL)

        setProgress(0.40, detail = "FanGraphs plate discipline…")
        fg_disc <- tryCatch(fetch_pd_fg_discipline(year), error = function(e) NULL)

        setProgress(0.55, detail = "FanGraphs Stuff+…")
        fg_stuff <- tryCatch(fetch_pd_fg_stuff(year), error = function(e) NULL)

        setProgress(0.70, detail = "FanGraphs PitchingBot…")
        fg_bot <- tryCatch(fetch_pd_fg_pitchingbot(year), error = function(e) NULL)

        setProgress(0.85, detail = "Building dashboard…")
        df_full <- build_pit_dashboard(sv_custom, fg_dash, fg_disc, fg_stuff, fg_bot)

        if (is.null(df_full) || nrow(df_full) == 0) {
          rv$error   <- "No data returned. Try a different season or lower the IP filter."
          rv$loading <- FALSE
          return()
        }

        rv$data    <- df_full
        rv$loading <- FALSE

        player_choices <- sort(df_full$player_name[!is.na(df_full$player_name)])
        updateSelectizeInput(session, "compare_players",
                             choices = player_choices, selected = character(0),
                             server  = TRUE)
      })
    })

    # Reactive filtered data (apply min IP after fetch)
    filtered <- reactive({
      df <- rv$data
      if (is.null(df)) return(NULL)
      min_ip <- input$min_ip
      df[!is.na(df$ip) & df$ip >= min_ip, , drop = FALSE]
    })

    # Debounced search (300 ms)
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

    # Stat selector: render after data is loaded
    output$stat_selector_ui <- renderUI({
      if (is.null(rv$data)) return(NULL)

      card_css <- setNames(
        paste0("pd-selector-card-", names(PD_STAT_GROUPS)),
        names(PD_STAT_GROUPS)
      )

      div(
        class = "pd-selector-row",
        lapply(names(PD_STAT_GROUPS), function(grp_name) {
          grp     <- PD_STAT_GROUPS[[grp_name]]
          choices <- setNames(grp$ids, PD_COLS$label[match(grp$ids, PD_COLS$id)])
          div(
            class = paste("pd-selector-card", card_css[[grp_name]]),
            div(class = "pd-selector-card-title", grp$label),
            checkboxGroupInput(
              ns(paste0("show_", grp_name)),
              label    = NULL,
              choices  = choices,
              selected = setdiff(grp$ids, PD_DEFAULT_UNCHECKED),
              inline   = FALSE
            )
          )
        })
      )
    })

    # Combined selected stat columns
    selected_stats <- reactive({
      cols <- character(0)
      for (g in names(PD_STAT_GROUPS)) {
        inp <- input[[paste0("show_", g)]]
        if (!is.null(inp)) {
          cols <- c(cols, inp)
        } else {
          cols <- c(cols, setdiff(PD_STAT_GROUPS[[g]]$ids, PD_DEFAULT_UNCHECKED))
        }
      }
      cols
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
          sprintf("✓ %d pitchers loaded · %d shown at ≥%d IP · %d match “%s”",
                  n_full, n_filter, input$min_ip, n_search, q)
        } else {
          sprintf("✓ %d pitchers loaded · %d shown at ≥%d IP",
                  n_full, n_filter, input$min_ip)
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
        p("Click 'Fetch Pitcher Data' to load stats.")))
      DT::dataTableOutput(ns("dt"))
    })

    output$dt <- DT::renderDataTable({
      df <- filtered_search()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      la <- compute_pit_league_avg(filtered())
      render_pit_dashboard_dt(df, la, selected_cols = selected_stats())
    }, server = TRUE)

    # ── Compare tab ──────────────────────────────────────────────────────────
    output$compare_ui <- renderUI({
      if (is.null(rv$data)) {
        return(div(class = "sps-empty",
                   p("Fetch data first, then search for pitchers above.")))
      }
      DT::dataTableOutput(ns("compare_dt"))
    })

    output$compare_dt <- DT::renderDataTable({
      df  <- rv$data
      sel <- input$compare_players
      if (is.null(df) || length(sel) == 0) return(NULL)
      sel_df <- df[df$player_name %in% sel, , drop = FALSE]
      if (nrow(sel_df) == 0) return(NULL)
      render_pit_dashboard_compare_dt(sel_df, selected_cols = selected_stats())
    }, server = FALSE)
  })
}
