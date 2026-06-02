suppressPackageStartupMessages({
  library(DT)
  library(jsonlite)
})

# ── Constants ──────────────────────────────────────────────────────────────────

TRATER_FG_USER_AGENT <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

TRATER_FG_BASE <- "https://www.fangraphs.com/api/leaders/major-league/data"

# Scoring direction: +1 = higher is better, -1 = lower is better
TRATER_SCORE_COLS <- c("HR", "R", "BB%", "K%", "wOBA")
TRATER_DIRS       <- c(HR = 1, R = 1, `BB%` = 1, `K%` = -1, wOBA = 1)

# Full team names (without city) keyed by FanGraphs abbreviation
TRATER_TEAM_NAMES <- c(
  ARI = "Diamondbacks", AZ  = "Diamondbacks",
  ATL = "Braves",
  BAL = "Orioles",
  BOS = "Red Sox",
  CHC = "Cubs",
  CWS = "White Sox",  CHW = "White Sox",
  CIN = "Reds",
  CLE = "Guardians",
  COL = "Rockies",
  DET = "Tigers",
  HOU = "Astros",
  KC  = "Royals",     KCR = "Royals",
  LAA = "Angels",
  LAD = "Dodgers",
  MIA = "Marlins",
  MIL = "Brewers",
  MIN = "Twins",
  NYM = "Mets",
  NYY = "Yankees",
  ATH = "Athletics",  OAK = "Athletics",
  PHI = "Phillies",
  PIT = "Pirates",
  SD  = "Padres",     SDP = "Padres",
  SEA = "Mariners",
  SF  = "Giants",     SFG = "Giants",
  STL = "Cardinals",
  TB  = "Rays",       TBR = "Rays",
  TEX = "Rangers",
  TOR = "Blue Jays",
  WSH = "Nationals",  WSN = "Nationals", WAS = "Nationals"
)

# ── URL builder ────────────────────────────────────────────────────────────────

trater_build_url <- function(season = 2026, month = 0,
                              start_date = NULL, end_date = NULL) {
  # type=c,6,11,12,34,35,50 is a custom stat selection matching the user's
  # FanGraphs leaderboard: PA(6), HR(11), R(12), BB%(34), K%(35), wOBA(50)
  # month=0 = full season (same as the reference leaderboard URL)
  url <- paste0(
    TRATER_FG_BASE,
    "?pos=all&stats=bat&lg=all&qual=0&ind=0",
    "&team=0,ts&rost=0&players=0",
    "&type=c,6,11,12,34,35,50",
    "&season=", season, "&season1=", season,
    "&month=", month,
    "&pageitems=50&pagenum=1"
  )
  if (!is.null(start_date)) url <- paste0(url, "&startdate=", start_date)
  if (!is.null(end_date))   url <- paste0(url, "&enddate=",   end_date)
  url
}

# ── FanGraphs JSON fetch ───────────────────────────────────────────────────────

trater_fg_fetch <- function(url) {
  fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
}

# ── Parse ──────────────────────────────────────────────────────────────────────

trater_parse <- function(result) {
  if (!isTRUE(result$ok) || is.null(result$payload))
    stop("Fetch failed: ", result$error %||% "unknown error")

  p  <- result$payload
  df <- if (is.data.frame(p))      p      else
        if (is.data.frame(p$data)) p$data else
        NULL
  if (is.null(df) || nrow(df) == 0)
    stop("No team rows in API response")

  cn_orig  <- names(df)
  cn_lower <- tolower(cn_orig)          # case-insensitive exact match; NO stripping
  col_raw <- function(...) {
    idx <- match(tolower(c(...)), cn_lower)   # first candidate that exists wins
    idx <- idx[!is.na(idx)]
    if (!length(idx)) return(rep(NA_character_, nrow(df)))
    as.character(df[[cn_orig[idx[1L]]]])
  }
  col_num <- function(...) suppressWarnings(as.numeric(col_raw(...)))

  abbr <- trimws(col_raw("TeamNameAbb", "TeamName", "Team", "teamabbr"))

  out <- data.frame(
    abbr  = abbr,
    PA    = col_num("PA"),
    HR    = col_num("HR"),
    R     = col_num("R"),
    `BB%` = col_num("BB%"),
    `K%`  = col_num("K%"),
    wOBA  = col_num("wOBA"),
    check.names    = FALSE,
    stringsAsFactors = FALSE
  )
  out[!is.na(out$abbr) & nzchar(out$abbr), , drop = FALSE]
}

# ── Score ──────────────────────────────────────────────────────────────────────

# weights: named numeric vector with names matching TRATER_SCORE_COLS; defaults to 1 each
trater_score <- function(df, weights = NULL) {
  if (is.null(df) || nrow(df) == 0) return(df)

  if (is.null(weights))
    weights <- setNames(rep(1, length(TRATER_SCORE_COLS)), TRATER_SCORE_COLS)

  z_cols <- lapply(TRATER_SCORE_COLS, function(col) {
    x  <- suppressWarnings(as.numeric(df[[col]]))
    mu <- mean(x, na.rm = TRUE)
    sg <- sd(x,   na.rm = TRUE)
    if (is.na(sg) || sg == 0) return(rep(0, nrow(df)))
    w <- if (!is.null(weights[[col]]) && !is.na(weights[[col]])) weights[[col]] else 1
    w * TRATER_DIRS[[col]] * (x - mu) / sg
  })

  z_sum <- rowSums(do.call(cbind, z_cols), na.rm = TRUE)
  z_mu  <- mean(z_sum, na.rm = TRUE)
  z_sg  <- sd(z_sum,   na.rm = TRUE)

  df$team_rater_index <- if (is.na(z_sg) || z_sg == 0) {
    rep(100, nrow(df))
  } else {
    round(100 + (z_sum - z_mu) / z_sg * 10, 1)
  }

  df[order(-df$team_rater_index, na.last = TRUE), , drop = FALSE]
}

# ── Display formatter ──────────────────────────────────────────────────────────
# Column order: Abbr | Team Name | PA | HR | R | BB% | K% | wOBA | Team Rater

trater_format <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)

  team_name <- unname(TRATER_TEAM_NAMES[df$abbr])
  team_name[is.na(team_name)] <- df$abbr[is.na(team_name)]  # fallback to abbr

  data.frame(
    `#`           = seq_len(nrow(df)),          # rank = sort order of input (already sorted desc)
    Team          = team_name,
    Abbr          = df$abbr,
    PA            = as.integer(round(df$PA)),
    HR            = as.integer(round(df$HR)),
    R             = as.integer(round(df$R)),
    `BB%`         = round(df$`BB%` * 100, 1),
    `K%`          = round(df$`K%`  * 100, 1),
    wOBA          = round(df$wOBA, 3),
    `Team Rater`  = df$team_rater_index,
    check.names   = FALSE,
    stringsAsFactors = FALSE
  )
}

# ── DT renderer ───────────────────────────────────────────────────────────────

trater_render_dt <- function(df, caption = NULL) {
  if (is.null(df) || nrow(df) == 0) {
    return(datatable(
      data.frame(Message = "No data — click Fetch Team Stats."),
      rownames = FALSE,
      options  = list(dom = "t", ordering = FALSE)
    ))
  }

  # Team Rater is the last column (index 8, 0-based)
  score_col <- ncol(df) - 1L
  s_vals    <- df[["Team Rater"]]
  s_min  <- round(min(s_vals,    na.rm = TRUE), 2)
  s_med  <- round(median(s_vals, na.rm = TRUE), 2)
  s_max  <- round(max(s_vals,    na.rm = TRUE), 2)

  # SP Skillz colour gradient: navy [31,53,86] → off-white [238,245,236] → amber [183,115,67]
  created_row_js <- JS(sprintf(
    "function(row, data, index) {
       var score = parseFloat(data[%d]);
       if (isNaN(score)) return;
       var sMin = %s, sMed = %s, sMax = %s;
       var pct;
       if (sMed > sMin && score <= sMed) {
         pct = 0.5 * (score - sMin) / (sMed - sMin);
       } else if (sMax > sMed) {
         pct = 0.5 + 0.5 * (score - sMed) / (sMax - sMed);
       } else { pct = 0.5; }
       pct = Math.max(0, Math.min(1, pct));
       var c1=[31,53,86], c2=[238,245,236], c3=[183,115,67], ca, cb, t;
       if (pct <= 0.5) { t = pct * 2;       ca = c1; cb = c2; }
       else            { t = (pct - 0.5)*2; ca = c2; cb = c3; }
       var r=Math.round(ca[0]+(cb[0]-ca[0])*t);
       var g=Math.round(ca[1]+(cb[1]-ca[1])*t);
       var b=Math.round(ca[2]+(cb[2]-ca[2])*t);
       var lum = 0.2126*(r/255) + 0.7152*(g/255) + 0.0722*(b/255);
       var txt = lum < 0.45 ? '#ffffff' : '#172733';
       $('td:eq(%d)', row).css({'background-color':'rgb('+r+','+g+','+b+')','color':txt,'font-weight':'700'});
     }",
    score_col, s_min, s_med, s_max, score_col
  ))

  datatable(
    df,
    caption   = if (!is.null(caption))
      tags$caption(style = "color:#8a9a8f;font-size:0.78rem;text-align:left;padding:4px 0 8px;", caption),
    filter    = "none",
    rownames  = FALSE,
    selection = "none",
    class     = "pf-dt display compact nowrap",
    options   = list(
      pageLength = 32,
      dom        = "frtip",
      order      = list(list(score_col, "desc")),
      createdRow = created_row_js,
      columnDefs = list(
        list(className = "dt-center", targets = 0),          # #
        list(className = "dt-left",   targets = 1:2),        # Team, Abbr
        list(className = "dt-center", targets = 3:(score_col - 1)),
        list(className = "dt-center", targets = score_col)
      )
    )
  ) |>
    formatStyle("#",     color = "#8a9a8f", fontSize = "0.82rem") |>
    formatStyle("Team",  fontWeight = "650", color = "#172733") |>
    formatStyle("Abbr",  color = "#4a5a4f",  fontSize = "0.82rem") |>
    formatStyle(c("PA", "HR", "R"),  color = "#4a5a4f") |>
    formatStyle(c("BB%", "K%", "wOBA"), color = "#4a5a4f") |>
    formatStyle("Team Rater", textAlign = "center")
}

# ── Module UI ──────────────────────────────────────────────────────────────────

traterUI <- function(id) {
  ns <- NS(id)

  div(
    class = "tr-page",

    # Page header
    div(
      class = "pf-header",
      div(class  = "pf-header-eyebrow", "Collinmyshot"),
      h1(class   = "pf-title", "Team Rater"),
      p(class    = "pf-subtitle",
        "MLB team offense ranked by composite z-score \u2014 HR, R, BB%, K%, and wOBA.",
        tags$br(),
        "Index: 100\u00a0=\u00a0league average \u00b7 \u00b110\u00a0pts\u00a0\u2248\u00a0\u00b11\u00a0standard deviation."
      )
    ),

    # Controls row — season toggle
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season"),
        div(class = "pf-toggle",
            radioButtons(ns("season"), NULL,
                         choices  = c("2025" = "2025", "2026" = "2026"),
                         selected = "2026", inline = TRUE))
      )
    ),

    # Stat weights row
    div(
      class = "sps-weight-row",
      tags$span(class = "sps-weight-row-label", "Stat weights"),
      div(class = "sps-weight-item",
          tags$span(class = "sps-weight-label", "HR"),
          numericInput(ns("w_hr"),   NULL, value = 1, min = 0, max = 5, step = 0.5, width = "68px")),
      div(class = "sps-weight-item",
          tags$span(class = "sps-weight-label", "R"),
          numericInput(ns("w_r"),    NULL, value = 1, min = 0, max = 5, step = 0.5, width = "68px")),
      div(class = "sps-weight-item",
          tags$span(class = "sps-weight-label", "BB%"),
          numericInput(ns("w_bb"),   NULL, value = 1, min = 0, max = 5, step = 0.5, width = "68px")),
      div(class = "sps-weight-item",
          tags$span(class = "sps-weight-label", "K%"),
          numericInput(ns("w_k"),    NULL, value = 1, min = 0, max = 5, step = 0.5, width = "68px")),
      div(class = "sps-weight-item",
          tags$span(class = "sps-weight-label", "wOBA"),
          numericInput(ns("w_woba"), NULL, value = 1, min = 0, max = 5, step = 0.5, width = "68px")),
      actionButton(ns("reset_weights"), "Reset", class = "btn-sps-day",
                   style = "margin-left:6px;")
    ),

    # Fetch row
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch Team Stats",
                   class = "btn btn-pag-generate",
                   icon  = icon("rotate")),
      div(class = "sps-status status-shell", textOutput(ns("status"), inline = TRUE))
    ),

    # Sub-tabs
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4CA"), "Full Season"),
        value = "full",
        div(class = "sps-tab-body", uiOutput(ns("full_ui")))
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F551"), "Last 30"),
        value = "l30",
        div(class = "sps-tab-body", uiOutput(ns("l30_ui")))
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(
          tags$span(class = "dl-tab-icon tr-ha-badge tr-ha-l", "L"),
          "vs LHP"
        ),
        value = "lhp",
        div(class = "sps-tab-body", uiOutput(ns("lhp_ui")))
      ),

      nav_panel(
        title = tagList(
          tags$span(class = "dl-tab-icon tr-ha-badge tr-ha-r", "R"),
          "vs RHP"
        ),
        value = "rhp",
        div(class = "sps-tab-body", uiOutput(ns("rhp_ui")))
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F9FE"), "Compare"),
        value = "compare",
        div(
          class = "sps-tab-body",
          uiOutput(ns("compare_ui"))
        )
      )
    )
  )
}

# ── Module Server ──────────────────────────────────────────────────────────────

traterServer <- function(id, fetch_trigger = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # Raw (unscored) data — fetched once, re-scored reactively on weight changes
    rv <- reactiveValues(
      raw_full = NULL,
      raw_l30  = NULL,
      raw_vlhp = NULL,
      raw_vrhp = NULL,
      ytd_h    = NULL,   # individual hitter YTD stats (for Compare tab)
      ytd_p    = NULL,   # individual pitcher YTD stats (for Compare tab)
      status      = "Click \u2018Fetch Team Stats\u2019 to load.",
      fetch_state = "none"   # "none" | "ok" | "error"
    )

    output$status <- renderText({ rv$status })

    # ── Weights reactive ──────────────────────────────────────────────────────
    weights <- reactive({
      c(
        HR    = max(0, input$w_hr   %||% 1),
        R     = max(0, input$w_r    %||% 1),
        `BB%` = max(0, input$w_bb   %||% 1),
        `K%`  = max(0, input$w_k    %||% 1),
        wOBA  = max(0, input$w_woba %||% 1)
      )
    })

    observeEvent(input$reset_weights, {
      updateNumericInput(session, "w_hr",   value = 1)
      updateNumericInput(session, "w_r",    value = 1)
      updateNumericInput(session, "w_bb",   value = 1)
      updateNumericInput(session, "w_k",    value = 1)
      updateNumericInput(session, "w_woba", value = 1)
    })

    # ── Scored reactives (re-score on weight change without re-fetching) ──────
    scored_full <- reactive({ trater_score(rv$raw_full, weights()) })
    scored_l30  <- reactive({ trater_score(rv$raw_l30,  weights()) })
    scored_vlhp <- reactive({ trater_score(rv$raw_vlhp, weights()) })
    scored_vrhp <- reactive({ trater_score(rv$raw_vrhp, weights()) })

    # Helper: fetch + parse one split, return raw df or NULL (no scoring here)
    fetch_split <- function(season, month) {
      tryCatch({
        url <- trater_build_url(season = season, month = month)
        res <- trater_fg_fetch(url)
        df  <- trater_parse(res)
        if (!is.null(df) && nrow(df) > 0) df else NULL
      }, error = function(e) NULL)
    }

    do_fetch <- function(season) {
      withProgress(message = "Fetching team stats…", value = 0, {
        rv$status      <- "Fetching…"
        rv$fetch_state <- "none"
        rv$raw_full    <- rv$raw_l30 <- rv$raw_vlhp <- rv$raw_vrhp <- NULL
        rv$ytd_h     <- rv$ytd_p <- NULL
        incProgress(0.25, detail = "Season to Date…")
        rv$raw_full  <- fetch_split(season, month = 0)
        incProgress(0.25, detail = "Last 30 Days…")
        rv$raw_l30   <- fetch_split(season, month = 3)
        incProgress(0.2, detail = "vs LHP…")
        rv$raw_vlhp  <- fetch_split(season, month = 13)
        incProgress(0.2, detail = "vs RHP…")
        rv$raw_vrhp  <- fetch_split(season, month = 14)
        # Fetch individual player YTD stats for Compare tab
        incProgress(0.1, detail = "Individual stats…")
        tryCatch({
          h <- auc_fetch_ytd("bat")
          p <- auc_fetch_ytd("pit")
          if (!is.null(p)) p$role <- classify_role(p)
          rv$ytd_h <- h
          rv$ytd_p <- p
        }, error = function(e) NULL)
        if (!is.null(rv$raw_full) && nrow(rv$raw_full) > 0) {
          rv$status      <- sprintf("%d teams — fetched %s", nrow(rv$raw_full), format(Sys.time(), "%I:%M %p"))
          rv$fetch_state <- "ok"
        } else {
          rv$status      <- paste0("Fetch failed — ", format(Sys.time(), "%I:%M %p"))
          rv$fetch_state <- "error"
        }
      })
    }
    observeEvent(input$fetch, {
      do_fetch(as.integer(input$season %||% "2026"))
    })

    # ── Full Season tab ──────────────────────────────────────────────────────
    output$full_ui <- renderUI({
      if (!is.null(rv$raw_full) && nrow(rv$raw_full) > 0)
        return(DTOutput(ns("full_dt")))
      if (rv$fetch_state == "error")
        return(div(class = "sps-empty",
          p("\u26a0\ufe0f Fetch failed. This may be a temporary issue \u2014 please try again.")))
      div(class = "sps-empty", p("Click \u2018Fetch Team Stats\u2019 to populate."))
    })
    output$full_dt <- renderDT({ trater_render_dt(trater_format(scored_full())) })

    # ── Last 30 Days tab ─────────────────────────────────────────────────────
    output$l30_ui <- renderUI({
      if (is.null(rv$raw_l30))
        return(div(class = "sps-empty",
          p("Last 30 days data unavailable."),
          p(class = "tr-note", "L30 splits require active regular-season games.")))
      DTOutput(ns("l30_dt"))
    })
    output$l30_dt <- renderDT({
      trater_render_dt(trater_format(scored_l30()),
        caption = paste("Last 30 days:",
          format(Sys.Date() - 30L, "%b %d"), "\u2013", format(Sys.Date(), "%b %d, %Y")))
    })

    # ── vs LHP tab ───────────────────────────────────────────────────────────
    output$lhp_ui <- renderUI({
      if (is.null(rv$raw_vlhp))
        return(div(class = "sps-empty",
          p("vs LHP data unavailable."),
          p(class = "tr-note", "Handedness splits require active regular-season games.")))
      DTOutput(ns("lhp_dt"))
    })
    output$lhp_dt <- renderDT({
      trater_render_dt(trater_format(scored_vlhp()),
        caption = paste("Team batting stats vs left-handed pitchers,", input$season %||% "2026", "season"))
    })

    # ── vs RHP tab ───────────────────────────────────────────────────────────
    output$rhp_ui <- renderUI({
      if (is.null(rv$raw_vrhp))
        return(div(class = "sps-empty",
          p("vs RHP data unavailable."),
          p(class = "tr-note", "Handedness splits require active regular-season games.")))
      DTOutput(ns("rhp_dt"))
    })
    output$rhp_dt <- renderDT({
      trater_render_dt(trater_format(scored_vrhp()),
        caption = paste("Team batting stats vs right-handed pitchers,", input$season %||% "2026", "season"))
    })

    # ── Compare tab ───────────────────────────────────────────────────────────

    output$compare_ui <- renderUI({
      if (is.null(rv$ytd_h) && is.null(rv$ytd_p))
        return(div(class = "sps-empty",
                   p("Click \u2018Fetch Team Stats\u2019 to load player data for comparison.")))
      navset_pill(
        id = ns("cmp_type"),
        nav_panel(
          title = "Hitters", value = "cmp_h",
          div(class = "sps-my-wrap2",
            div(class = "sps-my-header",
              numericInput(ns("n_cmp_h"), "Players", value = 5L,
                           min = 2L, max = 12L, step = 1L, width = "80px"),
              div(class = "sps-my-io",
                actionButton(ns("clear_cmp_h"), "Clear All",
                             class = "btn-outline-secondary")
              )
            ),
            uiOutput(ns("cmp_h_slots_ui")),
            DTOutput(ns("cmp_h_dt"))
          )
        ),
        nav_panel(
          title = "Pitchers", value = "cmp_p",
          div(class = "sps-my-wrap2",
            div(class = "sps-my-header",
              numericInput(ns("n_cmp_p"), "Players", value = 5L,
                           min = 2L, max = 12L, step = 1L, width = "80px"),
              div(class = "sps-my-io",
                actionButton(ns("clear_cmp_p"), "Clear All",
                             class = "btn-outline-secondary")
              )
            ),
            uiOutput(ns("cmp_p_slots_ui")),
            DTOutput(ns("cmp_p_dt"))
          )
        )
      )
    })

    # ── Hitter compare ────────────────────────────────────────────────────────

    cmp_h_pool <- reactive({
      d <- rv$ytd_h
      if (is.null(d)) return(character(0))
      sort(unique(d$name[!is.na(d$name) & nzchar(d$name)]))
    })

    prev_n_cmp_h <- reactiveVal(5L)

    mk_cmp_h_slot <- function(i) {
      pool <- isolate(cmp_h_pool())
      div(id = paste0("cmp_h_slot_wrap_", i), class = "sps-my-slot",
        selectizeInput(
          ns(paste0("cmp_h_", i)),
          label = NULL,
          choices  = c(setNames("", ""), setNames(pool, pool)),
          selected = "",
          options  = list(placeholder = paste("Hitter", i),
                          allowEmptyOption = TRUE,
                          maxOptions = 1000L)
        )
      )
    }

    output$cmp_h_slots_ui <- renderUI({
      n   <- max(2L, min(12L, as.integer(input$n_cmp_h %||% 5L)))
      old <- prev_n_cmp_h()
      prev_n_cmp_h(n)
      if (n > old)
        for (i in seq(old + 1L, n))
          insertUI(selector = paste0("#", ns("cmp_h_slots_ui"), " > div:last-child"),
                   where = "afterEnd", ui = mk_cmp_h_slot(i), immediate = TRUE)
      else if (n < old)
        for (i in seq(old, n + 1L))
          removeUI(selector = paste0("#cmp_h_slot_wrap_", i), immediate = TRUE)
      tagList(lapply(seq_len(n), mk_cmp_h_slot))
    })

    # Update choices when pool changes
    observeEvent(cmp_h_pool(), {
      pool <- cmp_h_pool()
      n    <- prev_n_cmp_h()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("cmp_h_", i),
                             choices = c(setNames("", ""), setNames(pool, pool)),
                             selected = isolate(input[[paste0("cmp_h_", i)]] %||% ""),
                             server = TRUE)
    })

    observeEvent(input$clear_cmp_h, {
      n <- prev_n_cmp_h()
      pool <- isolate(cmp_h_pool())
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("cmp_h_", i),
                             choices  = c(setNames("", ""), setNames(pool, pool)),
                             selected = "")
    })

    cmp_h_selected <- reactive({
      n <- prev_n_cmp_h()
      Filter(nzchar, vapply(seq_len(n), function(i) {
        v <- input[[paste0("cmp_h_", i)]]; if (is.null(v)) "" else v
      }, character(1)))
    })

    output$cmp_h_dt <- renderDT(server = FALSE, {
      sel <- cmp_h_selected()
      dat <- rv$ytd_h
      if (length(sel) == 0 || is.null(dat)) return(NULL)
      sub <- dat[dat$name %in% sel, , drop = FALSE]
      if (nrow(sub) == 0) return(NULL)
      # Preserve selection order; handle duplicate names (keep first match)
      sub <- sub[match(sel, sub$name), , drop = FALSE]
      sub <- sub[!is.na(sub$name), , drop = FALSE]
      keep <- intersect(c("name","team","pa","hr","r","rbi","sb","avg","obp"), names(sub))
      disp <- sub[, keep, drop = FALSE]
      labs <- c(name="Player", team="Team", pa="PA", hr="HR", r="R",
                rbi="RBI", sb="SB", avg="AVG", obp="OBP")
      nm <- ifelse(names(disp) %in% names(labs), labs[names(disp)], toupper(names(disp)))
      n_c <- ncol(disp)
      dt <- datatable(disp, rownames = FALSE, colnames = nm, filter = "none",
                      selection = "none",
                      class = "pf-dt display compact nowrap",
                      options = list(dom = "t", ordering = FALSE,
                                     autoWidth = FALSE, scrollX = TRUE)) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733") |>
        DT::formatStyle("team", color = "#4a5a4f", fontSize = "0.82rem")
      if ("avg" %in% names(disp)) dt <- DT::formatRound(dt, "avg", digits = 3)
      if ("obp" %in% names(disp)) dt <- DT::formatRound(dt, "obp", digits = 3)
      if ("hr"  %in% names(disp)) dt <- DT::formatRound(dt, "hr",  digits = 1)
      if ("r"   %in% names(disp)) dt <- DT::formatRound(dt, "r",   digits = 1)
      if ("rbi" %in% names(disp)) dt <- DT::formatRound(dt, "rbi", digits = 1)
      if ("sb"  %in% names(disp)) dt <- DT::formatRound(dt, "sb",  digits = 1)
      dt
    })

    # ── Pitcher compare ───────────────────────────────────────────────────────

    cmp_p_pool <- reactive({
      d <- rv$ytd_p
      if (is.null(d)) return(character(0))
      sort(unique(d$name[!is.na(d$name) & nzchar(d$name)]))
    })

    prev_n_cmp_p <- reactiveVal(5L)

    mk_cmp_p_slot <- function(i) {
      pool <- isolate(cmp_p_pool())
      div(id = paste0("cmp_p_slot_wrap_", i), class = "sps-my-slot",
        selectizeInput(
          ns(paste0("cmp_p_", i)),
          label = NULL,
          choices  = c(setNames("", ""), setNames(pool, pool)),
          selected = "",
          options  = list(placeholder = paste("Pitcher", i),
                          allowEmptyOption = TRUE,
                          maxOptions = 1000L)
        )
      )
    }

    output$cmp_p_slots_ui <- renderUI({
      n   <- max(2L, min(12L, as.integer(input$n_cmp_p %||% 5L)))
      old <- prev_n_cmp_p()
      prev_n_cmp_p(n)
      if (n > old)
        for (i in seq(old + 1L, n))
          insertUI(selector = paste0("#", ns("cmp_p_slots_ui"), " > div:last-child"),
                   where = "afterEnd", ui = mk_cmp_p_slot(i), immediate = TRUE)
      else if (n < old)
        for (i in seq(old, n + 1L))
          removeUI(selector = paste0("#cmp_p_slot_wrap_", i), immediate = TRUE)
      tagList(lapply(seq_len(n), mk_cmp_p_slot))
    })

    observeEvent(cmp_p_pool(), {
      pool <- cmp_p_pool()
      n    <- prev_n_cmp_p()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("cmp_p_", i),
                             choices = c(setNames("", ""), setNames(pool, pool)),
                             selected = isolate(input[[paste0("cmp_p_", i)]] %||% ""),
                             server = TRUE)
    })

    observeEvent(input$clear_cmp_p, {
      n <- prev_n_cmp_p()
      pool <- isolate(cmp_p_pool())
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("cmp_p_", i),
                             choices  = c(setNames("", ""), setNames(pool, pool)),
                             selected = "")
    })

    cmp_p_selected <- reactive({
      n <- prev_n_cmp_p()
      Filter(nzchar, vapply(seq_len(n), function(i) {
        v <- input[[paste0("cmp_p_", i)]]; if (is.null(v)) "" else v
      }, character(1)))
    })

    output$cmp_p_dt <- renderDT(server = FALSE, {
      sel <- cmp_p_selected()
      dat <- rv$ytd_p
      if (length(sel) == 0 || is.null(dat)) return(NULL)
      sub <- dat[dat$name %in% sel, , drop = FALSE]
      if (nrow(sub) == 0) return(NULL)
      sub <- sub[match(sel, sub$name), , drop = FALSE]
      sub <- sub[!is.na(sub$name), , drop = FALSE]
      keep <- intersect(c("name","team","role","ip","k","w","sv","hd","era","whip"), names(sub))
      disp <- sub[, keep, drop = FALSE]
      labs <- c(name="Player", team="Team", role="Role", ip="IP", k="K",
                w="W", sv="SV", hd="HD", era="ERA", whip="WHIP")
      nm <- ifelse(names(disp) %in% names(labs), labs[names(disp)], toupper(names(disp)))
      dt <- datatable(disp, rownames = FALSE, colnames = nm, filter = "none",
                      selection = "none",
                      class = "pf-dt display compact nowrap",
                      options = list(dom = "t", ordering = FALSE,
                                     autoWidth = FALSE, scrollX = TRUE)) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733") |>
        DT::formatStyle("team", color = "#4a5a4f", fontSize = "0.82rem")
      if ("role" %in% names(disp)) dt <- DT::formatStyle(dt, "role", color = "#4a5a4f", fontSize = "0.82rem")
      if ("ip"   %in% names(disp)) dt <- DT::formatRound(dt, "ip",   digits = 1)
      if ("k"    %in% names(disp)) dt <- DT::formatRound(dt, "k",    digits = 1)
      if ("w"    %in% names(disp)) dt <- DT::formatRound(dt, "w",    digits = 1)
      if ("sv"   %in% names(disp)) dt <- DT::formatRound(dt, "sv",   digits = 1)
      if ("hd"   %in% names(disp)) dt <- DT::formatRound(dt, "hd",   digits = 1)
      if ("era"  %in% names(disp)) dt <- DT::formatRound(dt, "era",  digits = 2)
      if ("whip" %in% names(disp)) dt <- DT::formatRound(dt, "whip", digits = 3)
      dt
    })

    # ── External trigger (e.g. from Streamonator Fetch Probables) ────────────
    if (!is.null(fetch_trigger)) {
      observeEvent(fetch_trigger(), {
        req(fetch_trigger() > 0)
        do_fetch(as.integer(input$season %||% "2026"))
      }, ignoreInit = TRUE)
    }

    # ── Return reactive data for Streamonator consumption ────────────────────
    return(reactive({
      list(
        std  = scored_full(),
        l30  = scored_l30(),
        vlhp = scored_vlhp(),
        vrhp = scored_vrhp()
      )
    }))
  })
}
