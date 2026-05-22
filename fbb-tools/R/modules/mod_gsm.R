suppressPackageStartupMessages({ library(DT); library(jsonlite) })
if (!exists("%||%")) `%||%` <- function(a, b) if (!is.null(a)) a else b

# ── Constants ─────────────────────────────────────────────────────────────────

GSM_SEASONS <- c("2021", "2022", "2023", "2024", "2025", "2026")

GSM_SEASON_DATES <- list(
  `2021` = c("2021-04-01", "2021-10-03"),
  `2022` = c("2022-04-07", "2022-10-05"),
  `2023` = c("2023-03-30", "2023-10-01"),
  `2024` = c("2024-03-20", "2024-09-29"),
  `2025` = c("2025-03-27", "2025-09-28"),
  `2026` = c("2026-03-26", "2026-09-27")
)

GSM_MIN_GS_DEFAULT <- 5L

GSM_MLB_ID_TO_ABR <- c(
  `108`="LAA", `109`="AZ",  `110`="BAL", `111`="BOS", `112`="CHC",
  `113`="CIN", `114`="CLE", `115`="COL", `116`="DET", `117`="HOU",
  `118`="KCR", `119`="LAD", `120`="WSH", `121`="NYM", `133`="ATH",
  `134`="PIT", `135`="SDP", `136`="SEA", `137`="SFG", `138`="STL",
  `139`="TBR", `140`="TEX", `141`="TOR", `142`="MIN", `143`="PHI",
  `144`="ATL", `145`="CHW", `146`="MIA", `147`="NYY", `158`="MIL"
)

GSM_DISPLAY_COLS <- c(
  "rank", "pitcher_name", "team", "gs",
  "gsm_avg", "gsm_pct",
  "ip_rate", "k_rate", "er_rate", "whip_rate"
)

GSM_DISPLAY_NAMES <- c(
  "RK", "Player", "Team", "GS",
  "Avg GSM", "GSM%",
  "IP 5+%", "K Rate%", "ER%", "WHIP%"
)

GSM_GLOSSARY <- list(
  list(
    term = "GSM Score",
    def  = "Good Start Metric: 0–4 composite. One point each for: IP ≥5, K ≥ floor(IP)–1, ER (sliding scale by IP), WHIP ≤1.18.",
    href = NULL
  ),
  list(
    term = "ER (sliding)",
    def  = "Earned run threshold scales with innings: ≥6 IP → ≤2 ER; 5.x IP → ≤3 ER; 4.x IP → ≤2 ER; <4 IP → ≤1 ER.",
    href = NULL
  ),
  list(
    term = "Avg GSM",
    def  = "Mean Good Start Metric score across all starts. Higher = more consistently strong outings.",
    href = NULL
  ),
  list(
    term = "GSM%",
    def  = "Percentage of starts with a GSM of 3 or higher (a ‘good start’).",
    href = NULL
  )
)

# ── Helper: IP string to decimal ─────────────────────────────────────────────

gsm_ip_to_dec <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  whole <- trunc(x)
  frac  <- round((x - whole) * 10)
  whole + frac / 3
}

# ── Data fetching ─────────────────────────────────────────────────────────────

gsm_fetch_season <- function(season, progress = NULL) {
  yr <- as.character(season)
  d1 <- GSM_SEASON_DATES[[yr]][1]
  d2 <- GSM_SEASON_DATES[[yr]][2]

  # For current/future seasons, cap end date at today
  today <- Sys.Date()
  if (as.Date(d2) > today) d2 <- as.character(today)
  if (as.Date(d1) > today) return(NULL)

  if (!is.null(progress)) progress$set(message = sprintf("Fetching %s schedule...", yr), value = 0.1)

  sched_url <- paste0(
    "https://statsapi.mlb.com/api/v1/schedule",
    "?sportId=1&gameType=R&startDate=", d1, "&endDate=", d2
  )
  sched_raw <- tryCatch(fromJSON(sched_url, simplifyVector = FALSE), error = function(e) NULL)
  if (is.null(sched_raw) || !length(sched_raw$dates)) return(NULL)

  game_index <- do.call(rbind, lapply(sched_raw$dates, function(day) {
    do.call(rbind, lapply(day$games, function(g) {
      if (!identical(tryCatch(g$status$abstractGameState, error = function(e) ""), "Final")) return(NULL)
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

  if (!nrow(game_index)) return(NULL)

  if (!is.null(progress)) progress$set(message = sprintf("Fetching %s boxscores (%d games)...", yr, nrow(game_index)), value = 0.2)

  parse_side <- function(side, ha, meta) {
    pitchers <- side$pitchers
    if (!length(pitchers)) return(NULL)
    sp_id  <- as.character(pitchers[[1]])
    player <- side$players[[paste0("ID", sp_id)]]
    if (is.null(player)) return(NULL)
    ps <- tryCatch(player$stats$pitching, error = function(e) NULL)
    if (is.null(ps)) return(NULL)
    ip_dec <- gsm_ip_to_dec(ps$inningsPitched %||% NA)
    team_id <- if (ha == "home") meta$home_id else meta$away_id
    data.frame(
      game_pk      = meta$game_pk,
      game_date    = meta$game_date,
      pitcher_id   = as.integer(sp_id),
      pitcher_name = trimws(player$person$fullName %||% NA_character_),
      team         = unname(GSM_MLB_ID_TO_ABR[as.character(team_id)]),
      ip  = ip_dec,
      er  = suppressWarnings(as.integer(ps$earnedRuns  %||% NA)),
      h   = suppressWarnings(as.integer(ps$hits        %||% NA)),
      bb  = suppressWarnings(as.integer(ps$baseOnBalls %||% NA)),
      k   = suppressWarnings(as.integer(ps$strikeOuts  %||% NA)),
      stringsAsFactors = FALSE
    )
  }

  starter_rows <- list()
  n_games <- nrow(game_index)
  for (i in seq_len(n_games)) {
    pk <- game_index$game_pk[i]
    box <- tryCatch(
      fromJSON(sprintf("https://statsapi.mlb.com/api/v1/game/%d/boxscore", pk),
               simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.null(box) || is.null(box$teams)) next
    for (ha in c("home", "away")) {
      row <- tryCatch(parse_side(box$teams[[ha]], ha, game_index[i, ]), error = function(e) NULL)
      if (!is.null(row) && nzchar(row$pitcher_name %||% ""))
        starter_rows[[length(starter_rows) + 1L]] <- row
    }
    if (!is.null(progress) && (i %% 100 == 0 || i == n_games))
      progress$set(
        message = sprintf("Fetching %s boxscores... %d/%d", yr, i, n_games),
        value   = 0.2 + 0.7 * (i / n_games)
      )
  }

  if (!length(starter_rows)) return(NULL)
  starts <- do.call(rbind, starter_rows)
  rownames(starts) <- NULL

  # Compute GSM components (4-component, no Win)
  starts$whip    <- ifelse(!is.na(starts$ip) & starts$ip > 0, (starts$h + starts$bb) / starts$ip, Inf)
  starts$ip_ok   <- !is.na(starts$ip) & starts$ip >= 5
  starts$k_ok    <- !is.na(starts$k)  & !is.na(starts$ip) & starts$k >= (floor(starts$ip) - 1)
  starts$er_ok   <- !is.na(starts$er) & !is.na(starts$ip) & (
    (starts$ip >= 6.0                        & starts$er <= 2) |
    (starts$ip >= 5.0 & starts$ip < 6.0     & starts$er <= 3) |
    (starts$ip >= 4.0 & starts$ip < 5.0     & starts$er <= 2) |
    (starts$ip <  4.0                        & starts$er <= 1)
  )
  starts$whip_ok <- !is.na(starts$whip) & starts$whip <= 1.18
  starts$gsm     <- as.integer(starts$ip_ok) + as.integer(starts$k_ok) +
    as.integer(starts$er_ok) + as.integer(starts$whip_ok)

  starts$season <- as.integer(season)
  starts
}

# ── Aggregate to pitcher-level leaderboard ───────────────────────────────────

gsm_aggregate <- function(starts, min_gs = GSM_MIN_GS_DEFAULT) {
  if (is.null(starts) || !nrow(starts)) return(NULL)

  # Use most recent team per pitcher
  team_last <- tapply(starts$team, starts$pitcher_id, function(x) x[length(x)])

  agg <- do.call(rbind, lapply(split(starts, starts$pitcher_id), function(df) {
    data.frame(
      pitcher_id   = df$pitcher_id[1],
      pitcher_name = df$pitcher_name[1],
      team         = unname(team_last[as.character(df$pitcher_id[1])]),
      gs           = nrow(df),
      gsm_avg      = round(mean(df$gsm, na.rm = TRUE), 2),
      gsm_pct      = round(100 * mean(df$gsm >= 3, na.rm = TRUE), 1),
      ip_rate      = round(100 * mean(df$ip_ok, na.rm = TRUE), 1),
      k_rate       = round(100 * mean(df$k_ok, na.rm = TRUE), 1),
      er_rate      = round(100 * mean(df$er_ok, na.rm = TRUE), 1),
      whip_rate    = round(100 * mean(df$whip_ok, na.rm = TRUE), 1),
      stringsAsFactors = FALSE
    )
  }))
  rownames(agg) <- NULL

  agg <- agg[agg$gs >= min_gs, ]
  agg <- agg[order(-agg$gsm_avg, -agg$gsm_pct), ]
  agg$rank <- seq_len(nrow(agg))
  agg
}

# ── DT rendering ─────────────────────────────────────────────────────────────

gsm_render_dt <- function(dat) {
  if (is.null(dat) || !nrow(dat)) {
    return(datatable(
      data.frame(Message = "No data — click Fetch Data to load."),
      rownames = FALSE, options = list(dom = "t")
    ))
  }

  display <- dat[, GSM_DISPLAY_COLS, drop = FALSE]
  names(display) <- GSM_DISPLAY_NAMES

  score_col <- which(GSM_DISPLAY_NAMES == "Avg GSM") - 1L
  s_vals <- dat$gsm_avg
  s_min  <- round(min(s_vals, na.rm = TRUE), 2)
  s_med  <- round(median(s_vals, na.rm = TRUE), 2)
  s_max  <- round(max(s_vals, na.rm = TRUE), 2)

  dt <- datatable(
    display,
    rownames  = FALSE,
    filter    = "none",
    selection = "none",
    options   = list(
      dom            = "t",
      ordering       = TRUE,
      pageLength     = nrow(display),
      scrollX        = TRUE,
      scrollY        = "calc(100vh - 360px)",
      scrollCollapse = FALSE,
      order          = list(list(0L, "asc")),
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
         }",
        score_col, s_min, s_med, s_max, score_col
      )),
      columnDefs = list(
        list(className = "dt-center", targets = "_all"),
        list(width = "42px",  targets = 0L),
        list(width = "160px", targets = 1L),
        list(width = "50px",  targets = 2L),
        list(width = "42px",  targets = 3L),
        list(width = "68px",  targets = 4L),
        list(width = "60px",  targets = 5L),
        list(width = "62px",  targets = 6L),
        list(width = "62px",  targets = 7L),
        list(width = "62px",  targets = 8L),
        list(width = "62px",  targets = 9L)
      )
    ),
    class = "pf-dt display nowrap"
  ) |>
    formatStyle("Avg GSM", fontWeight = "700", textAlign = "center") |>
    formatStyle("RK",
                fontWeight = "600", color = "#7a8f7f",
                fontSize = "0.8rem", textAlign = "center") |>
    formatStyle("Player", fontWeight = "650", color = "#172733") |>
    formatStyle("Team", color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle("GS", color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle(c("GSM%", "IP 5+%", "K Rate%", "ER%", "WHIP%"),
                color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center")

  dt
}

# ── Module UI ─────────────────────────────────────────────────────────────────

gsmUI <- function(id) {
  ns <- NS(id)

  div(
    class = "spz-page",

    # Page header
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "Good Start Metric"),
      p(
        class = "pf-subtitle",
        "Start-level quality score (0–4) for every SP appearance.",
        tags$br(),
        "One point each for: IP ≥5, K ≥ floor(IP)–1, ER (sliding scale by IP), WHIP ≤1.18.",
        tags$br(),
        "GSM% = percentage of starts scoring 3+. Data from MLB Stats API boxscores."
      )
    ),

    # Season toggle + min GS + fetch
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season"),
        div(
          class = "pf-toggle",
          radioButtons(
            ns("season"),
            label    = NULL,
            choices  = setNames(GSM_SEASONS, GSM_SEASONS),
            selected = "2026",
            inline   = TRUE
          )
        )
      ),
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Min GS"),
        numericInput(ns("min_gs"), NULL, value = GSM_MIN_GS_DEFAULT,
                     min = 1, max = 40, step = 1, width = "72px")
      ),
      div(
        class = "pf-control-group",
        actionButton(ns("fetch"), "Fetch Data",
                     class = "btn btn-pag-generate", icon = icon("rotate-right")),
        div(class = "sps-status status-shell", textOutput(ns("status"), inline = TRUE))
      )
    ),

    # Search
    div(
      class = "pf-controls-row spz-search-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Search"),
        textInput(ns("search"), NULL, placeholder = "Player or team...", width = "220px")
      )
    ),

    # Leaderboard table
    div(
      class = "sps-tab-body",
      DTOutput(ns("table"), width = "100%")
    ),

    # Glossary
    div(
      class = "spz-glossary",
      div(class = "spz-glossary-title", "Metric Reference"),
      tags$dl(
        tagList(lapply(GSM_GLOSSARY, function(item) {
          tagList(
            tags$dt(item$term),
            tags$dd(item$def)
          )
        }))
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

gsmServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    rv <- reactiveValues(
      raw    = NULL,
      agg    = NULL,
      status = ""
    )

    output$status <- renderText(rv$status)

    observeEvent(input$fetch, {
      season <- input$season
      rv$status <- "Fetching..."

      progress <- shiny::Progress$new(session, min = 0, max = 1)
      on.exit(progress$close())
      progress$set(message = "Starting...", value = 0)

      starts <- tryCatch(
        gsm_fetch_season(season, progress = progress),
        error = function(e) {
          rv$status <- paste("Error:", conditionMessage(e))
          NULL
        }
      )

      if (is.null(starts)) {
        if (rv$status == "Fetching...") rv$status <- "No data available for this season."
        rv$raw <- NULL
        rv$agg <- NULL
        return()
      }

      progress$set(message = "Aggregating...", value = 0.95)

      rv$raw <- starts
      min_gs <- input$min_gs %||% GSM_MIN_GS_DEFAULT
      rv$agg <- gsm_aggregate(starts, min_gs = min_gs)
      rv$status <- sprintf("Loaded %d starts for %d pitchers (%s).",
                           nrow(starts),
                           if (!is.null(rv$agg)) nrow(rv$agg) else 0L,
                           season)
    })

    # Re-aggregate when min GS changes
    observeEvent(input$min_gs, {
      if (!is.null(rv$raw)) {
        min_gs <- input$min_gs %||% GSM_MIN_GS_DEFAULT
        rv$agg <- gsm_aggregate(rv$raw, min_gs = min_gs)
      }
    }, ignoreInit = TRUE)

    # Filtered data for search
    agg_filtered <- reactive({
      dat <- rv$agg
      if (is.null(dat) || !nrow(dat)) return(dat)

      q <- trimws(tolower(input$search %||% ""))
      if (nzchar(q)) {
        keep <- grepl(q, tolower(dat$pitcher_name), fixed = TRUE) |
          grepl(q, tolower(dat$team), fixed = TRUE)
        dat <- dat[keep, ]
        dat$rank <- seq_len(nrow(dat))
      }
      dat
    })

    output$table <- renderDT({
      gsm_render_dt(agg_filtered())
    }, server = FALSE)

  })
}
