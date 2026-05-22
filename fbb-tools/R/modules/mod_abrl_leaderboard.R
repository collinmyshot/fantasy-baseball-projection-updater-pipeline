# mod_abrl_leaderboard.R
# Leaderboard: Adjusted Barrels (aBrl) — interactive hitter-season explorer.
# Data source: adjusted_barrel_hitters.csv (built by build_adjusted_barrels.R)

# ── Data loader ──────────────────────────────────────────────────────────────

load_abrl_hitters_lb <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_hitters.csv")
  if (!file.exists(path)) return(NULL)
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  if ("player_name" %in% names(d)) {
    Encoding(d$player_name) <- "UTF-8"
  }
  d
}

load_abrl_metadata <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_metadata.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── JS: Savant-style cell coloring via createdRow ────────────────────────────
# Mirrors Park Factors pattern: two-segment linear interpolation,
# luminance-based text flip. Blue (cold) -> White -> Red (hot).
# For % Lost: reversed (high = blue = bad).

ABRL_CREATED_ROW_JS <- "
function(row, data, index) {
  // Savant palette: blue=[33,102,172], white=[255,255,255], red=[255,59,48]
  var c1 = [33,102,172], c2 = [255,255,255], c3 = [255,59,48];

  function applyColor(td, val, lo, mid, hi, reverse) {
    if (isNaN(val)) return;
    var pct;
    if (val <= lo)       pct = 0;
    else if (val >= hi)  pct = 1;
    else if (val <= mid) pct = 0.5 * (val - lo) / (mid - lo);
    else                 pct = 0.5 + 0.5 * (val - mid) / (hi - mid);
    if (reverse) pct = 1 - pct;
    var ca, cb, t;
    if (pct <= 0.5) { t = pct * 2;       ca = c1; cb = c2; }
    else            { t = (pct - 0.5)*2;  ca = c2; cb = c3; }
    var r = Math.round(ca[0] + (cb[0]-ca[0])*t);
    var g = Math.round(ca[1] + (cb[1]-ca[1])*t);
    var b = Math.round(ca[2] + (cb[2]-ca[2])*t);
    var lum = 0.2126*(r/255) + 0.7152*(g/255) + 0.0722*(b/255);
    var txt = lum < 0.45 ? '#ffffff' : '#172733';
    $(td).css({
      'background-color': 'rgb('+r+','+g+','+b+')',
      'color': txt,
      'font-weight': '700'
    });
  }

  // Column indices: #=0, Player=1, PA=2, BBE=3, Barrels=4, aBarrels=5,
  // Brl/BBE%=6, aBrl/BBE%=7, Diff=8, Brl EV=9, % Lost=10
  var cells = $('td', row);

  var brlVal  = parseFloat(data[6]);
  var abrlVal = parseFloat(data[7]);
  var diffVal = parseFloat(data[8]);
  var lostVal = parseFloat(data[10]);

  // Use the bounds from the table settings object
  var s = this.api().settings()[0].oInit._abrlBounds;
  if (s) {
    applyColor(cells[6],  brlVal,  s.brl_lo,  s.brl_mid,  s.brl_hi,  false);
    applyColor(cells[7],  abrlVal, s.abrl_lo, s.abrl_mid, s.abrl_hi, false);
    applyColor(cells[8],  diffVal, s.diff_lo, s.diff_mid, s.diff_hi, false);
    applyColor(cells[10], lostVal, s.lost_lo, s.lost_mid, s.lost_hi, true);
  }
}
"

# ── Module UI ────────────────────────────────────────────────────────────────

abrlLeaderboardUI <- function(id) {
  ns <- shiny::NS(id)

  shiny::tagList(
    shiny::div(
      style = "max-width: 1100px; margin: 0 auto; padding: 1rem;",

      # ── Header (PF style) ──
      shiny::h2(class = "pf-title", "Adjusted Barrels (aBrl)"),
      shiny::p(class = "pf-subtitle",
               "Season-adjusted barrel rates for all hitters. ",
               "See the ",
               shiny::tags$a(href = "#research_adj_barrel",
                             style = "color: var(--primary); font-weight: 600;",
                             "aBrl research article"),
               " for background, or explore the ",
               shiny::tags$a(href = "https://docs.google.com/spreadsheets/d/16WxkFwZYKcurMM0CJJr4l5mo1S6rs-yKnnr1ZfmoGjU/edit",
                             target = "_blank", style = "color: var(--primary); font-weight: 600;",
                             "companion spreadsheet"),
               " for the full dataset."),

      # ── Tab panel: Leaderboard / Player Trends ──
      shiny::div(style = "margin-top: 16px;",
      shiny::tabsetPanel(
        id = ns("abrl_tabs"), type = "pills",

        # ━━ Tab 1: Leaderboard ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        shiny::tabPanel("Leaderboard",
          shiny::div(style = "margin-top: 16px;",

            # ── Controls row (SPZ preset-row style) ──
            shiny::div(class = "spz-preset-row",
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "SEASON"),
                shiny::selectInput(ns("season"), label = NULL,
                                   choices = NULL, selected = NULL, width = "100px")
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "MIN PA"),
                shiny::numericInput(ns("min_pa"), NULL, value = 50, min = 0, step = 25, width = "72px")
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "MIN BRL"),
                shiny::numericInput(ns("min_brl"), NULL, value = 0, min = 0, step = 1, width = "72px")
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "MIN aBRL"),
                shiny::numericInput(ns("min_abrl"), NULL, value = 5, min = 0, step = 1, width = "72px")
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;",
                shiny::tags$span(class = "spz-preset-label", "RATE"),
                shiny::selectInput(ns("rate_basis"), label = NULL,
                                   choices = c("per BBE" = "bbe", "per PA" = "pa"),
                                   selected = "bbe", width = "100px")
              )
            ),

            # ── Stability indicator (dynamic) ──
            shiny::uiOutput(ns("stability_badge")),

            # ── Search (SPZ style) ──
            shiny::div(
              class = "pf-controls-row spz-search-row",
              shiny::div(
                class = "spz-search-wrap",
                shiny::tags$span(class = "spz-search-icon", shiny::HTML("&#x2315;")),
                shiny::textInput(ns("search"), label = NULL,
                                 placeholder = "Search player or team...", width = "100%")
              )
            ),

            # ── Legend (PF style) ──
            shiny::div(class = "pf-legend", style = "margin-bottom: 12px;",
              shiny::tags$span(class = "pf-legend-label pf-legend-left", shiny::HTML("&larr; Bad")),
              shiny::tags$span(class = "pf-legend-bar",
                               style = "background: linear-gradient(to right, rgb(33,102,172), rgb(255,255,255), rgb(255,59,48));"),
              shiny::tags$span(class = "pf-legend-label pf-legend-right", shiny::HTML("Good &rarr;"))
            ),

            # ── Table (PF table-wrap style) ──
            shiny::div(class = "pf-table-wrap",
                       DT::DTOutput(ns("lb_table"), width = "100%"))
          )
        ),

        # ━━ Tab 2: Player Trends ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        shiny::tabPanel("Player Trends",
          shiny::div(style = "margin-top: 16px;",

            # ── Controls ──
            shiny::div(class = "spz-preset-row",
              style = "flex-wrap: wrap; gap: 8px 0;",
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "PLAYER 1"),
                shiny::selectizeInput(ns("cmp_player1"), label = NULL,
                                      choices = NULL, width = "220px",
                                      options = list(placeholder = "Type a player name..."))
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;margin-right:16px;",
                shiny::tags$span(class = "spz-preset-label", "PLAYER 2"),
                shiny::selectizeInput(ns("cmp_player2"), label = NULL,
                                      choices = NULL, width = "220px",
                                      options = list(placeholder = "(optional)"))
              ),
              shiny::div(style = "display:inline-flex;align-items:center;gap:8px;",
                shiny::tags$span(class = "spz-preset-label", "SHOW"),
                shiny::checkboxGroupInput(ns("cmp_metrics"), label = NULL, inline = TRUE,
                  choices = c("aBrl/BBE" = "abrl_bbe",
                              "Brl/BBE"  = "brl_bbe",
                              "aBrl/PA"  = "abrl_pa",
                              "Brl/PA"   = "brl_pa"),
                  selected = "abrl_bbe")
              )
            ),

            # ── Chart (dynamic height) ──
            shiny::div(
              style = "margin-top: 12px; background: #fff; border: 1px solid #c9d7c5;
                       border-radius: 12px; padding: 16px 20px 18px;",
              shiny::uiOutput(ns("cmp_chart_ui"))
            ),

            # ── Season detail table ──
            shiny::div(style = "margin-top: 16px;",
              DT::DTOutput(ns("cmp_table"), width = "100%")
            )
          )
        )
      ))
    )
  )
}

# ── Module Server ────────────────────────────────────────────────────────────

abrlLeaderboardServer <- function(id) {
  shiny::moduleServer(id, function(input, output, session) {

    hitters_raw <- load_abrl_hitters_lb()
    meta_raw    <- load_abrl_metadata()

    # Populate season dropdown (most recent first)
    shiny::observe({
      req(hitters_raw)
      szns <- sort(unique(hitters_raw$season), decreasing = TRUE)
      shiny::updateSelectInput(session, "season",
                               choices = szns,
                               selected = szns[1])
    })

    # Stability badge — shows % to stabilization for the selected season
    output$stability_badge <- shiny::renderUI({
      req(input$season)
      szn <- as.integer(input$season)

      # Look up from metadata; default to 100% for completed seasons
      pct <- 100L
      if (!is.null(meta_raw)) {
        row <- meta_raw[meta_raw$season == szn, ]
        if (nrow(row) == 1) pct <- row$pct_stability
      }

      if (pct >= 100) {
        # Stable season — green checkmark
        shiny::div(
          style = "margin: 6px 0 8px 0; display: inline-flex; align-items: center; gap: 6px;
                   padding: 4px 12px; background: #e8f5e9; border: 1px solid #a5d6a7;
                   border-radius: 6px; font-size: 0.82rem; color: #2e7d32; font-weight: 600;",
          shiny::HTML("&#x2713;"),
          sprintf("%d: Stabilized", szn)
        )
      } else {
        # Partial season — progress bar with percentage
        bar_color <- if (pct >= 70) "#43a047" else if (pct >= 40) "#fb8c00" else "#e53935"
        shiny::div(
          style = "margin: 6px 0 8px 0; display: inline-flex; align-items: center; gap: 10px;
                   padding: 5px 12px; background: #fff8e1; border: 1px solid #ffe082;
                   border-radius: 6px; font-size: 0.82rem; color: #5d4037;",
          shiny::tags$span(style = "font-weight: 600;",
                           sprintf("%d: %d%% to stabilization", szn, pct)),
          shiny::div(
            style = "width: 100px; height: 8px; background: #e0e0e0; border-radius: 4px; overflow: hidden;",
            shiny::div(style = sprintf(
              "width: %d%%; height: 100%%; background: %s; border-radius: 4px;",
              pct, bar_color
            ))
          )
        )
      }
    })

    # Debounced search
    search_d <- shiny::debounce(shiny::reactive(input$search), 300)

    # Filtered data
    tbl_data <- shiny::reactive({
      req(hitters_raw, input$season, input$min_pa)

      basis <- input$rate_basis %||% "bbe"

      d <- hitters_raw |>
        dplyr::filter(season == as.integer(input$season),
                      !is.na(pa), pa >= input$min_pa,
                      tango_barrels >= (input$min_brl %||% 0),
                      adj_barrels   >= (input$min_abrl %||% 0))

      # Apply search filter (diacritic-insensitive)
      q <- trimws(search_d() %||% "")
      if (nchar(q) > 0) {
        q_ascii <- iconv(q, to = "ASCII//TRANSLIT", sub = "")
        names_ascii <- iconv(d$player_name, to = "ASCII//TRANSLIT", sub = "")
        mask <- grepl(q_ascii, names_ascii, ignore.case = TRUE) |
                grepl(q, d$team, ignore.case = TRUE)
        d <- d[mask, ]
      }

      # Compute rates based on selected denominator
      if (basis == "pa") {
        d <- d |>
          dplyr::mutate(
            brl_rate  = round(tango_barrels / pa * 100, 1),
            abrl_rate = round(adj_barrels / pa * 100, 1),
            diff_rate = round(adj_barrels / pa * 100 - tango_barrels / pa * 100, 1)
          )
      } else {
        d <- d |>
          dplyr::mutate(
            brl_rate  = round(tango_brl_pct, 1),
            abrl_rate = round(adj_brl_pct, 1),
            diff_rate = round(brl_diff, 1)
          )
      }

      # Column names reflect the rate basis
      brl_col  <- if (basis == "pa") "Brl/PA%"  else "Brl/BBE%"
      abrl_col <- if (basis == "pa") "aBrl/PA%" else "aBrl/BBE%"

      d |>
        dplyr::arrange(dplyr::desc(abrl_rate)) |>
        dplyr::mutate(`#` = dplyr::row_number()) |>
        dplyr::transmute(
          `#`,
          Player       = player_name,
          PA           = pa,
          BBE          = total_bbe,
          Barrels      = tango_barrels,
          aBarrels     = adj_barrels,
          !!brl_col   := brl_rate,
          !!abrl_col  := abrl_rate,
          Diff         = diff_rate,
          `Brl EV`     = barrel_ev_mean,
          `% Lost`     = round(pct_lost, 0)
        )
    })

    # ── Player Trends: populate player dropdowns ──
    shiny::observe({
      req(hitters_raw)
      players <- sort(unique(hitters_raw$player_name))
      # Build choices with ASCII search keys for diacritic-insensitive matching
      player_choices <- data.frame(
        value = players,
        label = players,
        search = iconv(players, to = "ASCII//TRANSLIT", sub = ""),
        stringsAsFactors = FALSE
      )
      selectize_opts <- list(
        placeholder = "Type a player name...",
        valueField = "value", labelField = "label",
        searchField = c("label", "search"),
        render = I("{option: function(item, escape) {return '<div>' + escape(item.label) + '</div>'}}")
      )
      shiny::updateSelectizeInput(session, "cmp_player1",
                                  choices = player_choices,
                                  selected = "",
                                  server = TRUE,
                                  options = selectize_opts)
      selectize_opts2 <- selectize_opts
      selectize_opts2$placeholder <- "(optional)"
      shiny::updateSelectizeInput(session, "cmp_player2",
                                  choices = player_choices,
                                  selected = "",
                                  server = TRUE,
                                  options = selectize_opts2)
    })

    # All available seasons (fixed x-axis, excl 2020)
    all_seasons <- setdiff(2015:2026, 2020)

    # Helper: get one player's data with gap-filling
    get_player_rates <- function(player_name) {
      if (is.null(player_name) || nchar(player_name) == 0) return(NULL)

      d <- hitters_raw |>
        dplyr::filter(player_name == !!player_name) |>
        dplyr::arrange(season) |>
        dplyr::mutate(
          abrl_bbe = round(adj_brl_pct, 1),
          brl_bbe  = round(tango_brl_pct, 1),
          abrl_pa  = round(adj_barrels / pa * 100, 1),
          brl_pa   = round(tango_barrels / pa * 100, 1)
        )
      if (nrow(d) == 0) return(NULL)

      # Fill missing seasons with NA (creates line breaks)
      full_grid <- data.frame(season = all_seasons)
      d <- dplyr::left_join(full_grid, d, by = "season")
      d$player_name <- player_name
      d
    }

    # Combined data for both players
    cmp_data <- shiny::reactive({
      req(hitters_raw)
      p1 <- input$cmp_player1
      p2 <- input$cmp_player2

      d1 <- get_player_rates(p1)
      d2 <- get_player_rates(p2)

      if (is.null(d1) && is.null(d2)) return(NULL)
      dplyr::bind_rows(d1, d2)
    })

    # Dynamic chart height
    cmp_chart_height <- shiny::reactive({
      p2 <- input$cmp_player2
      if (!is.null(p2) && nchar(p2) > 0) 720 else 400
    })

    output$cmp_chart_ui <- shiny::renderUI({
      ns <- session$ns
      shiny::plotOutput(ns("cmp_chart"), height = paste0(cmp_chart_height(), "px"))
    })

    # Player trend chart
    output$cmp_chart <- shiny::renderPlot({
      d <- cmp_data()
      metrics <- input$cmp_metrics

      if (is.null(d) || is.null(metrics) || length(metrics) == 0) {
        plot.new()
        text(0.5, 0.5, "Select a player and at least one metric",
             cex = 1.2, col = "#888")
        return()
      }

      metric_palette <- c(
        abrl_bbe = "#2f7d3a",
        brl_bbe  = "#8b5e3c",
        abrl_pa  = "#1565c0",
        brl_pa   = "#c62828"
      )
      metric_labels <- c(
        abrl_bbe = "aBrl/BBE%",
        brl_bbe  = "Brl/BBE%",
        abrl_pa  = "aBrl/PA%",
        brl_pa   = "Brl/PA%"
      )

      # Reshape to long, drop NAs
      long <- d |>
        dplyr::select(season, player_name, dplyr::all_of(metrics)) |>
        tidyr::pivot_longer(cols = -c(season, player_name),
                            names_to = "metric", values_to = "rate") |>
        dplyr::filter(!is.na(rate)) |>
        dplyr::mutate(
          metric_label = factor(metric_labels[metric], levels = metric_labels[metrics])
        )

      if (nrow(long) == 0) {
        plot.new()
        text(0.5, 0.5, "No data for selected player(s)", cex = 1.2, col = "#888")
        return()
      }

      players <- unique(d$player_name[!is.na(d$pa)])
      n_players <- length(players)

      p <- ggplot2::ggplot(long, ggplot2::aes(x = season, y = rate,
                                               colour = metric_label,
                                               group = metric_label)) +
        ggplot2::geom_line(linewidth = 1.3) +
        ggplot2::geom_point(size = 3.5) +
        ggrepel::geom_text_repel(
          ggplot2::aes(label = sprintf("%.1f", rate)),
          size = 3.3, show.legend = FALSE,
          nudge_y = 0.4, segment.color = "#ccc", segment.size = 0.3,
          min.segment.length = 0.2, box.padding = 0.3,
          direction = "y", seed = 42
        ) +
        ggplot2::scale_colour_manual(
          values = metric_palette[metrics] |> stats::setNames(metric_labels[metrics]),
          name = NULL
        ) +
        ggplot2::scale_x_continuous(breaks = all_seasons, labels = all_seasons) +
        ggplot2::scale_y_continuous(expand = ggplot2::expansion(mult = c(0.05, 0.15))) +
        ggplot2::labs(x = "Season", y = "Barrel Rate %") +
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

      # Facet by player if two selected
      if (n_players > 1) {
        # Order facets: player 1 on top
        long$player_name <- factor(long$player_name, levels = players)
        p <- p + ggplot2::facet_wrap(~ player_name, ncol = 1, scales = "free_y")
      } else {
        p <- p + ggplot2::ggtitle(paste0(players[1], ": Barrel Rate by Season"))
      }

      p
    }, res = 96, bg = "white")

    # Player trend detail table
    output$cmp_table <- DT::renderDT({
      d <- cmp_data()
      if (is.null(d)) return(NULL)

      # Only show seasons that have data for at least one player
      tbl <- d |>
        dplyr::filter(!is.na(pa)) |>
        dplyr::transmute(
          Player   = player_name,
          Season   = season,
          Team     = team,
          PA       = pa,
          BBE      = total_bbe,
          Barrels  = tango_barrels,
          aBarrels = adj_barrels,
          `Brl/BBE%`  = brl_bbe,
          `aBrl/BBE%` = abrl_bbe,
          `Brl/PA%`   = brl_pa,
          `aBrl/PA%`  = abrl_pa,
          `Brl EV`    = ifelse(is.na(barrel_ev_mean), "--", sprintf("%.1f", barrel_ev_mean)),
          `% Lost`    = ifelse(is.na(pct_lost), "--", sprintf("%.0f%%", pct_lost))
        ) |>
        dplyr::arrange(Player, Season)

      DT::datatable(
        tbl, rownames = FALSE, selection = "none",
        options = list(dom = "t", paging = FALSE, ordering = FALSE,
                       columnDefs = list(
                         list(className = "dt-center", targets = "_all")
                       )),
        class = "compact stripe"
      )
    })

    output$lb_table <- DT::renderDT({
      req(tbl_data())
      d <- tbl_data()

      # Compute bounds for the JS coloring (rate columns are always at positions 7/8)
      brl_vals  <- d[[7]]
      abrl_vals <- d[[8]]
      diff_vals <- d$Diff
      lost_vals <- d$`% Lost`

      q <- function(x, p) as.numeric(stats::quantile(x, p, na.rm = TRUE))

      bounds <- list(
        brl_lo  = q(brl_vals, 0.05),  brl_mid  = q(brl_vals, 0.5),  brl_hi  = q(brl_vals, 0.95),
        abrl_lo = q(abrl_vals, 0.05), abrl_mid = q(abrl_vals, 0.5), abrl_hi = q(abrl_vals, 0.95),
        diff_lo = q(diff_vals, 0.05), diff_mid = q(diff_vals, 0.5), diff_hi = q(diff_vals, 0.95),
        lost_lo = q(lost_vals, 0.05), lost_mid = q(lost_vals, 0.5), lost_hi = q(lost_vals, 0.95)
      )

      DT::datatable(
        d, rownames = FALSE, escape = FALSE,
        selection = "none",
        options = list(
          dom        = "tip",
          pageLength = nrow(d),
          ordering   = TRUE,
          order      = list(list(7, "desc")),
          `_abrlBounds` = bounds,
          createdRow = DT::JS(ABRL_CREATED_ROW_JS),
          columnDefs = list(
            list(className = "dt-center", targets = c(0, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
            list(className = "dt-left",   targets = 1),
            list(width = "36px",  targets = 0),
            list(width = "150px", targets = 1),
            list(width = "55px",  targets = c(2, 3, 4, 5)),
            list(width = "80px",  targets = c(6, 7, 8, 9, 10))
          )
        ),
        class = "pf-dt display nowrap"
      ) |>
        DT::formatStyle(
          "#",
          color      = "#8a9a8f",
          fontWeight = "400",
          fontSize   = "0.8rem"
        ) |>
        DT::formatStyle(
          "Player",
          fontWeight = "650",
          color      = "#172733"
        ) |>
        DT::formatStyle(
          c("PA", "BBE", "Barrels", "aBarrels", "Brl EV"),
          color    = "#4a5a4f",
          fontSize = "0.85rem"
        )
    })
  })
}
