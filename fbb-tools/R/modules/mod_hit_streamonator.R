suppressPackageStartupMessages({ library(DT) })

# ── Core build ─────────────────────────────────────────────────────────────────
# Input:  probables data frame (one row per pitcher start)
# Output: one row per BATTING team, sorted by Score descending
#
# Each row in probables represents a pitcher starting against an opponent.
# The BATTING team for that game is `opponent_team`.
# Park is always the HOME team's park:
#   home_away == "H" → pitcher is home  → park = pitcher_team
#   home_away == "A" → pitcher is away  → park = opponent_team

hit_stream_build <- function(probables, spz_std, spz_l30 = NULL, pf, tr_data = NULL,
                              week_start = NULL, week_end = NULL,
                              w_spz_std = 1, w_spz_l30 = 0,
                              w_g = 2, w_pitcher = 2, w_park = 1, w_team = 0.5) {

  if (is.null(probables) || nrow(probables) == 0) return(NULL)

  df <- probables
  if (!is.null(week_start)) df <- df[!is.na(df$date) & df$date >= as.Date(week_start), ]
  if (!is.null(week_end))   df <- df[!is.na(df$date) & df$date <= as.Date(week_end),   ]
  if (nrow(df) == 0) return(NULL)

  if (!"pitcher_throws" %in% names(df)) df$pitcher_throws <- NA_character_

  # ── Raw SP Skillz score for each pitcher ────────────────────────────────────
  # Use sp_skillz_score_stabilized directly (not the 100-indexed value) so that
  # averaging across a team's games preserves the real spread.  Indexing before
  # averaging collapses variance toward 100 — that's why the display was 99-101.
  spz_raw_vec <- function(spz) {
    if (is.null(spz) || nrow(spz) == 0 || !"player_name" %in% names(spz))
      return(rep(NA_real_, nrow(df)))
    col <- if ("sp_skillz_score_stabilized" %in% names(spz)) "sp_skillz_score_stabilized" else
           if ("sp_skillz_index"            %in% names(spz)) "sp_skillz_index"            else
           return(rep(NA_real_, nrow(df)))
    s <- suppressWarnings(as.numeric(spz[[col]]))
    s[stream_match_names(df$pitcher_name, spz$player_name)]
  }
  std_raw <- spz_raw_vec(spz_std)
  l30_raw <- spz_raw_vec(spz_l30)
  df$spz_raw     <- stream_score(std_raw, l30_raw, weights = c(w_spz_std, w_spz_l30))
  df$spz_placeholder <- is.na(df$spz_raw)
  mu_spz <- mean(df$spz_raw, na.rm = TRUE)
  if (is.nan(mu_spz) || is.na(mu_spz)) mu_spz <- 0
  df$spz_raw[df$spz_placeholder] <- mu_spz   # unknown pitchers = league avg

  # ── Park factor for each game ────────────────────────────────────────────────
  if (!is.null(pf) && nrow(pf) > 0) {
    lookup       <- ifelse(df$home_away == "A", df$opponent_team, df$pitcher_team)
    pi           <- match(lookup, pf$team_norm)
    df$park_factor <- round(pf$overall_pf_idx_100[pi], 1)
  } else {
    df$park_factor <- NA_real_
  }

  # ── Aggregate by batting team (opponent_team) ────────────────────────────────
  batting_teams <- sort(unique(df$opponent_team[!is.na(df$opponent_team) & nzchar(df$opponent_team)]))
  if (length(batting_teams) == 0) return(NULL)

  rows <- lapply(batting_teams, function(team) {
    tdf <- df[df$opponent_team == team, , drop = FALSE]
    n_g <- nrow(tdf)

    throws_up <- toupper(trimws(tdf$pitcher_throws))
    n_vl <- sum(!is.na(throws_up) & throws_up == "L")
    n_vr <- sum(!is.na(throws_up) & throws_up == "R")

    # Build matchup string: "3 vs BOS, 4 at ATH"
    # home_away is from the PITCHER's perspective:
    #   H = pitcher is home → batting team is away → "at [opp]"
    #   A = pitcher is away → batting team is home → "vs [opp]"
    sched     <- tdf[order(tdf$date), c("pitcher_team", "home_away"), drop = FALSE]
    runs      <- rle(sched$pitcher_team)
    cum_end   <- cumsum(runs$lengths)
    cum_start <- c(1L, cum_end[-length(cum_end)] + 1L)
    matchup_parts <- vapply(seq_along(runs$values), function(i) {
      ha     <- sched$home_away[cum_start[i]]
      prefix <- if (!is.na(ha) && toupper(ha) == "A") "vs" else "at"
      sprintf("%d %s %s", runs$lengths[i], prefix, runs$values[i])
    }, character(1))
    matchups <- paste(matchup_parts, collapse = ", ")

    avg_opp_spz_raw <- mean(tdf$spz_raw, na.rm = TRUE)
    wpf          <- round(mean(tdf$park_factor, na.rm = TRUE), 1)

    # Team Rater (batting team's offensive quality) — std only
    tr_idx         <- NA_real_
    tr_placeholder <- TRUE
    if (!is.null(tr_data$std) && nrow(tr_data$std) > 0) {
      ti <- match(stream_norm_team(team), stream_norm_team(tr_data$std$abbr))
      if (!is.na(ti)) { tr_idx <- tr_data$std$team_rater_index[ti]; tr_placeholder <- FALSE }
    }
    if (is.na(tr_idx)) tr_idx <- 100

    data.frame(
      team           = team,
      g              = n_g,
      matchups       = matchups,
      vl             = n_vl,
      vr             = n_vr,
      avg_opp_spz_raw = avg_opp_spz_raw,
      wpf            = wpf,
      team_rater     = round(tr_idx, 1),
      tr_placeholder = tr_placeholder,
      stringsAsFactors = FALSE
    )
  })

  res <- do.call(rbind, rows)

  # Re-index team-level raw averages to a 100-scale for display.
  # This preserves the actual spread across teams (e.g. 88–112) rather than
  # the collapsed 99–101 you get from averaging pre-indexed per-pitcher values.
  mu_t <- mean(res$avg_opp_spz_raw, na.rm = TRUE)
  sg_t <- sd(res$avg_opp_spz_raw, na.rm = TRUE)
  res$avg_opp_spz <- if (is.na(sg_t) || sg_t == 0) rep(100, nrow(res)) else
    round(100 + (res$avg_opp_spz_raw - mu_t) / sg_t * 10, 1)

  # ── Composite Score (z-score each component, combine, index to 100) ─────────
  z_safe <- function(x) {
    mu <- mean(x, na.rm = TRUE); sg <- sd(x, na.rm = TRUE)
    if (is.na(sg) || sg == 0) return(rep(0, length(x)))
    (x - mu) / sg
  }

  g_z       <- z_safe(res$g)
  pitcher_z <- z_safe(-res$avg_opp_spz_raw)     # negate raw: lower = better for hitters
  park_z    <- z_safe(res$wpf)
  team_z    <- z_safe(res$team_rater)

  raw <- w_g * g_z + w_pitcher * pitcher_z + w_park * park_z + w_team * team_z

  rs_mu <- mean(raw, na.rm = TRUE); rs_sg <- sd(raw, na.rm = TRUE)
  res$score <- if (is.na(rs_sg) || rs_sg == 0) rep(100, nrow(res)) else
    round(100 + (raw - rs_mu) / rs_sg * 10, 1)

  res[order(-res$score), , drop = FALSE]
}

# ── Display formatter ──────────────────────────────────────────────────────────

hit_stream_format_display <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)
  # Keep numeric values numeric so DataTables sorts correctly.
  # Hidden flag columns (.spz_tbd, .tr_ph) are used by a JS render function
  # to append '*' in display mode only — that way sort ignores the asterisk.
  data.frame(
    Team             = df$team,
    Matchups         = df$matchups,
    Games            = df$g,
    `G vs LHP`       = df$vl,
    `G vs RHP`       = df$vr,
    `Opp SP Skillz`  = df$avg_opp_spz,
    `Park Factor`    = df$wpf,
    `Team Offense`   = df$team_rater,
    Score            = df$score,
    `.tr_ph`         = as.integer(!is.na(df$tr_placeholder) & df$tr_placeholder),
    check.names      = FALSE,
    stringsAsFactors = FALSE
  )
}

# ── DT renderer ───────────────────────────────────────────────────────────────

hit_stream_render_dt <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(datatable(
      data.frame(Message = "No data. Click \u2018Fetch Schedule\u2019 to populate."),
      rownames = FALSE, options = list(dom = "t", ordering = FALSE)
    ))
  }

  scores <- suppressWarnings(as.numeric(df[["Score"]]))
  s_min  <- min(scores, na.rm = TRUE)
  s_max  <- max(scores, na.rm = TRUE)
  s_med  <- median(scores, na.rm = TRUE)

  # Column layout (0-based):
  # 0:Team 1:Matchups 2:Games 3:G vs LHP 4:G vs RHP
  # 5:Opp SP Skillz 6:Park Factor 7:Team Offense 8:Score  9:.tr_ph (hidden)
  score_col <- 8L
  tr_col    <- 7L
  tr_ph_col <- 9L

  datatable(
    df,
    filter    = "none",
    rownames  = FALSE,
    selection = "none",
    class     = "pf-dt display nowrap",
    options   = list(
      pageLength    = 35,
      scrollX       = TRUE,
      scrollY       = "calc(100vh - 420px)",
      scrollCollapse= TRUE,
      dom           = "ftp",
      order         = list(list(score_col, "desc")),
      createdRow    = JS(sprintf(
        "function(row, data, index) {
           var score = parseFloat(data[%d]);
           if (!isNaN(score)) {
             var sMin=%s, sMed=%s, sMax=%s, pct;
             if (sMed > sMin && score <= sMed) pct = 0.5*(score-sMin)/(sMed-sMin);
             else if (sMax > sMed) pct = 0.5+0.5*(score-sMed)/(sMax-sMed);
             else pct = 0.5;
             pct = Math.max(0, Math.min(1, pct));
             var c1=[31,53,86],c2=[238,245,236],c3=[183,115,67],ca,cb,t;
             if (pct<=0.5){t=pct*2;ca=c1;cb=c2;}else{t=(pct-0.5)*2;ca=c2;cb=c3;}
             var r=Math.round(ca[0]+(cb[0]-ca[0])*t);
             var g=Math.round(ca[1]+(cb[1]-ca[1])*t);
             var b=Math.round(ca[2]+(cb[2]-ca[2])*t);
             var lum=0.2126*(r/255)+0.7152*(g/255)+0.0722*(b/255);
             var txt=lum<0.45?'#ffffff':'#172733';
             $('td:eq(%d)',row).css({'background-color':'rgb('+r+','+g+','+b+')','color':txt});
           }
         }",
        score_col, s_min, s_med, s_max, score_col
      )),
      columnDefs = list(
        list(className = "dt-left",   targets = 0L),
        list(className = "dt-center", targets = c(seq.int(1L, 8L))),
        list(visible = FALSE, targets = tr_ph_col),
        list(width = "60px",  targets = 0L),    # Team
        list(width = "140px", targets = 1L),    # Matchups
        list(width = "46px",  targets = 2L),    # Games
        list(width = "68px",  targets = 3L),    # G vs LHP
        list(width = "68px",  targets = 4L),    # G vs RHP
        list(width = "100px", targets = 5L),    # Opp SP Skillz
        list(width = "90px",  targets = 6L),    # Park Factor
        list(width = "100px", targets = 7L),    # Team Offense
        list(width = "70px",  targets = 8L),    # Score
        # Append '*' to Team Offense in display mode only; sort uses raw numeric
        list(
          targets = tr_col,
          render  = JS(sprintf(
            "function(data, type, row) {
               if (type !== 'display') return data;
               return row[%d] > 0 ? data + '*' : String(data);
             }", tr_ph_col))
        )
      )
    )
  ) |>
    formatRound(c("Opp SP Skillz", "Park Factor", "Team Offense"), digits = 1) |>
    formatStyle("Score",      fontWeight = "700") |>
    formatStyle("Team",       fontWeight = "650", color = "#172733") |>
    formatStyle("Matchups",   color = "#4a5a4f", fontSize = "0.83rem", textAlign = "center") |>
    formatStyle(c("Games", "G vs LHP", "G vs RHP"),
                color = "#4a5a4f", textAlign = "center") |>
    formatStyle(c("Opp SP Skillz", "Park Factor", "Team Offense"),
                color = "#4a5a4f", textAlign = "center") |>
    # G vs RHP: 6-7 games = good (green), 1-3 = bad (red), 4-5 = neutral
    formatStyle("G vs RHP",
                backgroundColor = styleInterval(
                  c(3.5, 5.5),
                  c("rgba(200,60,60,0.18)", "transparent", "rgba(46,160,67,0.18)"))) |>
    # G vs LHP: 4+ games = good (green)
    formatStyle("G vs LHP",
                backgroundColor = styleInterval(
                  c(3.5),
                  c("transparent", "rgba(46,160,67,0.18)")))
}

# ── Placeholder note ───────────────────────────────────────────────────────────

hit_stream_placeholder_note <- tags$p(
  class = "sps-susp-note",
  tags$span(style = "font-weight:700;color:#b07a2a;", "* "),
  "Value set to 100 (league avg) \u2014 pitcher or team data not yet available. ",
  "Scores will update once enough games have been played."
)

# ── Module UI ──────────────────────────────────────────────────────────────────

hitStreamUI <- function(id) {
  ns <- NS(id)

  div(
    class = "sps-page",

    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "Hitter Streamonator"),
      p(class = "pf-subtitle",
        "Weekly team-level hitting matchup analysis \u2014 schedule \u00d7 SP Skillz \u00d7 Park Factor \u00d7 Team Offense.",
        tags$br(),
        "Score ranks each team\u2019s batting opportunity this week: more games, weaker pitching, hitter-friendly parks."
      )
    ),

    # ── Top toggles ───────────────────────────────────────────────────────────
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Week"),
        div(class = "pf-toggle",
            radioButtons(ns("which_week"), NULL,
                         choices  = c("Current Week" = "current", "Next Week" = "next"),
                         selected = "current", inline = TRUE)),
        div(class = "sps-week-label", textOutput(ns("week_label"), inline = TRUE))
      ),
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "SP Skillz"),
        div(class = "pf-toggle",
            radioButtons(ns("spz_year"), NULL,
                         choices  = c("2025" = "2025", "2026" = "2026"),
                         selected = "2026", inline = TRUE))
      )
    ),

    # ── Weights ───────────────────────────────────────────────────────────────
    div(
      class = "sps-weights-wrap",

      # Panel 1 — Component weights
      div(
        class = "sps-weights-panel",
        div(class = "sps-weights-panel-title", "Score Weights"),
        div(class = "sps-weights-panel-subtitle", "Set to 0 to exclude a component"),
        div(
          class = "sps-weight-row",
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Games"),
              numericInput(ns("w_g"),       NULL, 2,   0, 10, 0.5, width = "90px")),
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Pitcher Quality"),
              numericInput(ns("w_pitcher"), NULL, 2,   0, 10, 0.5, width = "90px")),
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Park Factor"),
              numericInput(ns("w_park"),    NULL, 1,   0, 10, 0.5, width = "90px")),
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Team Offense"),
              numericInput(ns("w_team"),    NULL, 0.5, 0, 10, 0.5, width = "90px"))
        )
      ),

      # Panel 2 — SP Skillz blend
      div(
        class = "sps-weights-panel",
        div(class = "sps-weights-panel-title", "Factor Weights"),
        div(class = "sps-weights-panel-subtitle", "Controls how SP Skillz is blended internally"),
        div(
          class = "sps-weight-row",
          div(
            class = "sps-weight-group",
            div(class = "sps-weight-group-label", "SP Skillz"),
            div(
              class = "sps-weight-row sps-weight-row-inner",
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Season to Date"),
                  numericInput(ns("w_spz_std"), NULL, 1, 0, 10, 0.5, width = "90px")),
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Last 30"),
                  numericInput(ns("w_spz_l30"), NULL, 1, 0, 10, 0.5, width = "90px"))
            )
          )
        )
      )
    ),

    # ── Fetch button + status ─────────────────────────────────────────────────
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch Schedule",
                   class = "btn btn-pag-generate",
                   icon  = icon("rotate")),
      div(class = "sps-status status-shell", textOutput(ns("status"), inline = TRUE))
    ),

    # ── Results table ─────────────────────────────────────────────────────────
    div(
      class = "sps-tab-body",
      uiOutput(ns("table_ui"))
    )
  )
}

# ── Module Server ──────────────────────────────────────────────────────────────

hitStreamServer <- function(id, spz_data_ext = NULL, team_rater_data = NULL,
                             spz_fetch_trigger = NULL, tr_fetch_trigger = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      probables    = NULL,
      park_factors = stream_load_pf(),
      status       = "Click \u2018Fetch Schedule\u2019 to load this week\u2019s matchups.",
      diag         = ""
    )

    # ── Week ──────────────────────────────────────────────────────────────────
    week <- reactive(stream_week_range(Sys.Date(), input$which_week %||% "current"))
    output$week_label <- renderText({ week()$label })

    # ── SP Skillz (live from module or CSV fallback) ───────────────────────────
    spz_data <- reactive({
      live <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
      if (!is.null(live$std)) return(live)
      yr <- input$spz_year %||% "2026"
      list(
        std = stream_load_spz(year = yr, period = "std"),
        l30 = stream_load_spz(year = yr, period = "l30")
      )
    })

    # ── Fetch ─────────────────────────────────────────────────────────────────
    output$status <- renderText({ rv$status })

    observeEvent(input$fetch, {
      withProgress(message = "Fetching schedule…", value = 0, {
        rv$status    <- "Fetching…"
        rv$probables <- NULL
        tryCatch({
          incProgress(0.1, detail = "Connecting…")
          raw    <- stream_fg_fetch(STREAM_PROBABLES_URL)
          incProgress(0.7, detail = "Parsing schedule…")
          parsed <- stream_parse_probables(raw)
          rv$probables <- parsed$data

          # Auto-advance to Next Week if current week is empty
          wk_cur <- stream_week_range(Sys.Date(), "current")
          in_cur <- !is.na(parsed$data$date) &
                    parsed$data$date >= wk_cur$start &
                    parsed$data$date <= wk_cur$end
          if (!any(in_cur) && (input$which_week %||% "current") == "current")
            updateRadioButtons(session, "which_week", selected = "next")

          n_games <- nrow(parsed$data)

          incProgress(0.1, detail = "Checking supporting data…")
          # SP Skillz: use session cache or trigger module fetch
          spz_live <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
          spz_note <- if (!is.null(spz_live$std)) "SP Skillz: cached" else {
            if (!is.null(spz_fetch_trigger)) spz_fetch_trigger(spz_fetch_trigger() + 1L)
            "SP Skillz: fetching…"
          }

          # Team Rater: use session cache or trigger module fetch
          tr_live <- if (is.function(team_rater_data)) team_rater_data() else NULL
          tr_note <- if (!is.null(tr_live$std)) "Team Rater: cached" else {
            if (!is.null(tr_fetch_trigger)) tr_fetch_trigger(tr_fetch_trigger() + 1L)
            "Team Rater: fetching…"
          }

          incProgress(0.1)
          rv$status <- sprintf(
            "%d games fetched — %s | %s | %s",
            n_games, format(Sys.time(), "%I:%M %p"), spz_note, tr_note
          )
        }, error = function(e) {
          rv$status <- paste0("Error: ", conditionMessage(e))
        })
      })
    })
    # ── Scored table ──────────────────────────────────────────────────────────
    scored <- reactive({
      req(rv$probables)
      wk  <- week()
      spz <- spz_data()
      tr  <- if (is.function(team_rater_data)) team_rater_data() else NULL
      hit_stream_build(
        rv$probables,
        spz_std    = spz$std,
        spz_l30    = spz$l30,
        pf         = rv$park_factors,
        tr_data    = tr,
        week_start = wk$start, week_end = wk$end,
        w_spz_std  = max(0, input$w_spz_std  %||% 1),
        w_spz_l30  = max(0, input$w_spz_l30  %||% 1),
        w_g        = max(0, input$w_g        %||% 2),
        w_pitcher  = max(0, input$w_pitcher  %||% 2),
        w_park     = max(0, input$w_park     %||% 1),
        w_team     = max(0, input$w_team     %||% 0.5)
      )
    })

    # ── Table output ──────────────────────────────────────────────────────────
    output$table_ui <- renderUI({
      if (is.null(rv$probables))
        return(div(class = "sps-empty",
                   p("Click \u2018Fetch Schedule\u2019 to load this week\u2019s team matchups.")))
      df <- scored()
      has_placeholder <- !is.null(df) && any(df$tr_placeholder, na.rm = TRUE)
      tagList(
        DTOutput(ns("table")),
        if (has_placeholder) hit_stream_placeholder_note
      )
    })

    output$table <- renderDT({
      df <- req(scored())
      hit_stream_render_dt(hit_stream_format_display(df))
    })

    # ── Return scored data for downstream modules (e.g. FAAB Helper) ─────────
    return(reactive({
      tryCatch(scored(), error = function(e) NULL)
    }))
  })
}
