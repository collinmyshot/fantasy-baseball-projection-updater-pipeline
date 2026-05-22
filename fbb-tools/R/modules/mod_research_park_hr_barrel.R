# mod_research_park_hr_barrel.R
# Research article: Park-Adjusting HR/Barrel Rate (2015-2025)

# ── Data loaders ─────────────────────────────────────────────────────────────

load_park_hr_barrel <- function() {
  path <- file.path("data", "processed", "park_hr_barrel.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

load_park_hr_barrel_yearly <- function() {
  path <- file.path("data", "processed", "park_hr_barrel_yearly.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── Chart theme ──────────────────────────────────────────────────────────────

park_brl_theme <- function() {
  ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.background    = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background   = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid.major   = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
      panel.grid.minor   = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
      axis.title  = ggplot2::element_text(size = 13, face = "bold"),
      axis.text   = ggplot2::element_text(size = 11),
      axis.text.y = ggplot2::element_text(size = 10),
      plot.title  = ggplot2::element_text(face = "bold", size = 15),
      plot.margin = ggplot2::margin(12, 18, 12, 12)
    )
}

PB_CARD <- "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;"

# ── Module UI ────────────────────────────────────────────────────────────────

parkHrBarrelUI <- function(id) {
  ns <- NS(id)

  div(
    class = "park-brl-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
       "Park-Adjusting HR/Barrel Rate"),
    p(style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "Statcast BBE data, 2015-2025 (excl. 2020). Regular season only."),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Background"),
      p("HR/Barrel rate (the ratio of home runs to barrels) is a popular metric for ",
        "evaluating whether a hitter is over- or under-performing their power output. ",
        "A hitter with a low HR/Barrel rate is often called \"unlucky\" and expected to ",
        "see positive HR regression. But this simple framing ignores a critical variable: ",
        tags$b("where"), " those barrels are being hit."),
      p("Parks dramatically influence HR/Barrel rates. A barrel in Cincinnati is worth ",
        "far more than one in San Francisco, and a hitter's HR/Brl should be evaluated ",
        "against their home park baseline, not the league average. ",
        "This analysis computes park-specific HR/Barrel rates using three components:"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$b("Earned HR/Brl:"), " % of barrels that were home runs. ",
                "Shows how efficiently each park converts barrels into HR."),
        tags$li(tags$b("Lucky HR/Brl:"), " % of home runs that came on non-barreled balls. ",
                "Identifies parks where weak contact still clears the fence."),
        tags$li(tags$b("Overall HR/Brl:"), " total HR / total barrels. ",
                "The combined effect: the \"true\" HR/Brl baseline for each park.")
      ),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Data & Methodology"),
      p(HTML("Barrels are identified using the Tango/Statcast definition: exit velocity &ge; 98 mph ",
        "with launch angle in the barrel zone (speed &times; 1.5 - angle &ge; 117, speed + angle &ge; 124, ",
        "angle between 4-50&deg;). "),
        "Data covers 1.2M batted ball events from 2015-2025 regular season games, excluding 2020. ",
        "Park is determined by the home team for each game."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "A Note on Barrel Definition Drift"),
      p("An important caveat: the barrel zone is a ", tags$b("fixed"), " geometric definition ",
        "calibrated against 2015 outcome data: the EV/LA combinations that produced ",
        ".500 BA and 1.500 SLG when Statcast launched. Unlike metrics such as xwOBA or Stuff+, ",
        "which are recalibrated each offseason, the barrel definition has never been updated."),
      p("This matters because barrel outcomes have drifted significantly. ",
        "As ",
        tags$a(href = "https://blogs.fangraphs.com/they-dont-make-barrels-like-they-used-to/",
               target = "_blank", style = "color: #2f7d3a;",
               "FanGraphs documented"),
        ", wOBA on non-HR barrels has fallen from .656 in 2015 to .508 in 2025, ",
        "driven by changes in ball aerodynamics and deeper outfield positioning. ",
        "A barrel today is defined by the same criteria as in 2015, ",
        "but the batted balls meeting that definition produce less value ",
        "because the run environment around it has changed."),
      p("For this analysis, the fixed definition is actually useful: it gives us a consistent ",
        "yardstick to measure how parks modulate barrel outcomes over time. ",
        HTML("The declining league-wide HR/Brl rate (76% in 2015 &rarr; 56% in 2025) is not a change in "),
        "what qualifies as a barrel; it reflects real changes in the game. And critically, ",
        "that decline is not uniform across parks, which is exactly what this article measures."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Key Findings"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$b("Cincinnati dominates."), " Great American Ball Park converts barrels to HR ",
                "at 65.6% (Earned) and 26.0% of its HR come on non-barrels (Lucky), yielding an ",
                "Overall HR/Brl of 88.6%, far above the league average of 67.3%."),
        tags$li(tags$b("San Francisco is the toughest HR park."), " Oracle Park has the lowest ",
                "Earned rate (43.3%) and a modest Lucky rate (16.3%), for an Overall of just 51.7%."),
        tags$li(tags$b("Some parks specialize in \"Lucky\" HR."), " Houston converts barrels ",
                "at a below-average rate (53.1%) but leads the league in Lucky HR (32.8%), ",
                "thanks to the Crawford Boxes. Similarly, NYY, CLE, and BOS have high Lucky rates."),
        tags$li(tags$b("League-wide HR/Brl has declined sharply:"), " from ~76% in 2015 ",
                "to ~56% in 2025, tracking the deadened ball era.")
      )
    ),

    # ── Section 1: Year-by-Year Baseline ─────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "League-Wide HR/Barrel by Season"),
    div(style = PB_CARD, plotOutput(ns("yearly_chart"), height = "340px")),
    DTOutput(ns("yearly_table"), width = "100%"),

    # ── Section 2: Park Leaderboard ──────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Park HR/Barrel Leaderboard"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "Toggle between the full dataset and the recent 3-year rolling window."),
    radioButtons(ns("period"), NULL,
                 choices = c("Full (2015-2025)" = "2015-2025",
                             "Rolling 3-Year (2023-2025)" = "2023-2025"),
                 selected = "2015-2025", inline = TRUE),
    DTOutput(ns("park_table"), width = "100%"),

    # ── Section 3: Visual ────────────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Earned vs Lucky HR/Barrel by Park"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "Earned (barrel-to-HR conversion) on the x-axis, Lucky (non-barrel HR share) on the y-axis. ",
      "Parks in the upper-right are hitter-friendly on both dimensions."),
    div(style = PB_CARD, plotOutput(ns("scatter_park"), height = "560px")),

    # ── Section 4: Overall HR/Brl bar chart ──────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Overall HR/Barrel Rate by Park"),
    radioButtons(ns("bar_period"), NULL,
                 choices = c("Full (2015-2025)" = "2015-2025",
                             "Rolling 3-Year (2023-2025)" = "2023-2025"),
                 selected = "2015-2025", inline = TRUE),
    div(style = PB_CARD, plotOutput(ns("bar_overall"), height = "560px")),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: Statcast BBE data via Baseball Savant, 2015-2025 (excl. 2020). ",
      HTML("Barrels defined per Tango/Statcast: EV &ge; 98 mph, LA in barrel zone. "),
      "Park = home team venue. Regular season only. ",
      "Original concept by @Batflipcrazy; methodology by @Collinmyshot."
    )
  )
}

# ── Module Server ────────────────────────────────────────────────────────────

parkHrBarrelServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    park_data <- load_park_hr_barrel()
    yearly_data <- load_park_hr_barrel_yearly()

    # ── Year-by-year chart ──────────────────────────────────────────────────

    output$yearly_chart <- renderPlot({
      req(yearly_data)
      d <- yearly_data

      ggplot2::ggplot(d, ggplot2::aes(x = season, y = hr_per_barrel)) +
        ggplot2::geom_line(linewidth = 1.2, colour = "#2f7d3a") +
        ggplot2::geom_point(size = 3, colour = "#2f7d3a") +
        ggplot2::geom_text(
          ggplot2::aes(label = sprintf("%.1f%%", hr_per_barrel)),
          vjust = -1, size = 3.5, colour = "#333"
        ) +
        ggplot2::scale_x_continuous(breaks = d$season) +
        ggplot2::scale_y_continuous(limits = c(40, 90)) +
        ggplot2::labs(
          x = "Season", y = "HR / Barrel %",
          title = "League-Wide HR/Barrel Rate by Season"
        ) +
        park_brl_theme() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 45, hjust = 1))
    }, bg = "white")

    output$yearly_table <- renderDT({
      req(yearly_data)
      d <- yearly_data |>
        dplyr::select(
          Season = season,
          Barrels = total_barrels,
          HR = total_hr,
          `HR/Brl %` = hr_per_barrel,
          `Earned %` = earned_pct,
          `Lucky %` = lucky_pct
        )
      datatable(d, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(d),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })

    # ── Park leaderboard ────────────────────────────────────────────────────

    output$park_table <- renderDT({
      req(park_data)
      period <- input$period
      d <- park_data[park_data$period == period, ]

      lg_earned <- round(sum(d$barrel_hr) / sum(d$total_barrels) * 100, 1)
      lg_lucky <- round(sum(d$nonbarrel_hr) / sum(d$total_hr) * 100, 1)
      lg_overall <- round(sum(d$total_hr) / sum(d$total_barrels) * 100, 1)

      tbl <- d |>
        dplyr::arrange(dplyr::desc(overall_pct)) |>
        dplyr::transmute(
          Park = park,
          Barrels = total_barrels,
          HR = total_hr,
          `Earned %` = earned_pct,
          `Lucky %` = lucky_pct,
          `Overall %` = overall_pct
        )

      lg_row <- data.frame(
        Park = "LG AVG", Barrels = sum(d$total_barrels), HR = sum(d$total_hr),
        `Earned %` = lg_earned, `Lucky %` = lg_lucky, `Overall %` = lg_overall,
        check.names = FALSE, stringsAsFactors = FALSE
      )
      tbl <- rbind(tbl, lg_row)

      datatable(tbl, rownames = FALSE, filter = "none", selection = "none",
                options = list(
                  dom = "t", ordering = TRUE, pageLength = nrow(tbl),
                  scrollX = FALSE,
                  columnDefs = list(list(className = "dt-center", targets = "_all"))
                ),
                class = "display compact nowrap") |>
        DT::formatStyle(
          columns = "Park",
          target = "row",
          fontWeight = DT::styleEqual("LG AVG", "bold"),
          backgroundColor = DT::styleEqual("LG AVG", "#f0f7ef")
        )
    })

    # ── Earned vs Lucky scatterplot ─────────────────────────────────────────

    output$scatter_park <- renderPlot({
      req(park_data)
      period <- input$period
      d <- park_data[park_data$period == period, ]

      lg_earned <- sum(d$barrel_hr) / sum(d$total_barrels) * 100
      lg_lucky <- sum(d$nonbarrel_hr) / sum(d$total_hr) * 100

      x_range <- range(d$earned_pct)
      y_range <- range(d$lucky_pct)
      x_pad <- diff(x_range) * 0.06
      y_pad <- diff(y_range) * 0.06

      x_lo <- x_range[1] - diff(x_range) * 0.15
      x_hi <- x_range[2] + diff(x_range) * 0.15
      y_lo <- y_range[1] - diff(y_range) * 0.15
      y_hi <- y_range[2] + diff(y_range) * 0.15

      quad_shades <- data.frame(
        xmin = c(lg_earned, x_lo,      lg_earned, x_lo),
        xmax = c(x_hi,     lg_earned,  x_hi,      lg_earned),
        ymin = c(lg_lucky,  lg_lucky,   y_lo,      y_lo),
        ymax = c(y_hi,      y_hi,       lg_lucky,  lg_lucky),
        fill = c("#2f7d3a", "#d4a843", "#3b6fa0", "#b74343")
      )

      ggplot2::ggplot(d, ggplot2::aes(x = earned_pct, y = lucky_pct)) +
        ggplot2::geom_rect(data = quad_shades, inherit.aes = FALSE,
          ggplot2::aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = fill),
          alpha = 0.07) +
        ggplot2::scale_fill_identity() +
        ggplot2::geom_vline(xintercept = lg_earned, linetype = "dashed", colour = "#aaa", linewidth = 0.5) +
        ggplot2::geom_hline(yintercept = lg_lucky, linetype = "dashed", colour = "#aaa", linewidth = 0.5) +
        ggplot2::annotate("text", x = x_hi - x_pad * 0.3, y = y_hi - y_pad * 0.3,
                          label = "Power Paradise", size = 4.5, colour = "#2f7d3a",
                          fontface = "bold.italic", alpha = 0.5, hjust = 1) +
        ggplot2::annotate("text", x = x_lo + x_pad * 0.3, y = y_hi - y_pad * 0.3,
                          label = "Cheap HR Park", size = 4.5, colour = "#b08c20",
                          fontface = "bold.italic", alpha = 0.5, hjust = 0) +
        ggplot2::annotate("text", x = x_hi - x_pad * 0.3, y = y_lo + y_pad * 0.3,
                          label = "Gotta Earn It", size = 4.5, colour = "#2d5a85",
                          fontface = "bold.italic", alpha = 0.5, hjust = 1) +
        ggplot2::annotate("text", x = x_lo + x_pad * 0.3, y = y_lo + y_pad * 0.3,
                          label = "Pitcher's Park", size = 4.5, colour = "#b74343",
                          fontface = "bold.italic", alpha = 0.5, hjust = 0) +
        mlbplotR::geom_mlb_logos(
          ggplot2::aes(team_abbr = park, width = 0.04),
          alpha = 0.9
        ) +
        ggplot2::labs(
          x = "Earned HR/Brl % (barrel to HR conversion)",
          y = "Lucky HR/Brl % (non-barrel HR share)",
          title = paste("Earned vs Lucky HR/Barrel by Park:", period)
        ) +
        park_brl_theme()
    }, bg = "white")

    # ── Overall HR/Brl bar chart ────────────────────────────────────────────

    output$bar_overall <- renderPlot({
      req(park_data)
      period <- input$bar_period
      d <- park_data[park_data$period == period, ]

      lg_overall <- sum(d$total_hr) / sum(d$total_barrels) * 100

      d <- d |> dplyr::arrange(overall_pct)
      d$park <- factor(d$park, levels = d$park)

      ggplot2::ggplot(d, ggplot2::aes(x = overall_pct, y = park)) +
        ggplot2::geom_col(
          fill = ifelse(d$overall_pct >= lg_overall, "#2f7d3a", "#b77343"),
          alpha = 0.7
        ) +
        ggplot2::geom_vline(xintercept = lg_overall, linetype = "dashed",
                            colour = "#555", linewidth = 0.6) +
        ggplot2::geom_text(
          ggplot2::aes(label = sprintf("%.1f%%", overall_pct)),
          hjust = -0.15, size = 3.2, colour = "#333"
        ) +
        ggplot2::annotate("text", x = lg_overall + 0.5, y = 1,
                          label = sprintf("LG AVG (%.1f%%)", lg_overall),
                          size = 3, colour = "#555", hjust = 0, fontface = "italic") +
        ggplot2::scale_x_continuous(expand = ggplot2::expansion(mult = c(0, 0.15))) +
        ggplot2::labs(
          x = "Overall HR/Barrel %", y = NULL,
          title = paste("Overall HR/Barrel Rate by Park:", period)
        ) +
        park_brl_theme()
    }, bg = "white")
  })
}
