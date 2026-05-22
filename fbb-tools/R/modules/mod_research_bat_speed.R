# mod_research_bat_speed.R
# Research article: Bat Speed, Exit Velocity & HR (2024-2025)

# ── Data loader ───────────────────────────────────────────────────────────────

load_bat_speed_data <- function() {
  path <- file.path("data", "processed", "bat_speed_research.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

load_bat_speed_correlations <- function() {
  path <- file.path("data", "processed", "bat_speed_correlations.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── Bucket helpers ────────────────────────────────────────────────────────────

BS_HR600_LABELS <- c("<=10", "10-15", "15-20", "20-25", "25-30", "30-40", "40+")
BS_MAXEV_LABELS <- c("<=109", "110-112", "112-115", "115+")

assign_bs_hr600_bucket <- function(hr600) {
  factor(cut(hr600, breaks = c(-Inf, 10, 15, 20, 25, 30, 40, Inf),
             labels = BS_HR600_LABELS, right = TRUE), levels = BS_HR600_LABELS)
}

assign_bs_maxev_bucket <- function(x) {
  factor(cut(x, breaks = c(-Inf, 109, 112, 115, Inf),
             labels = BS_MAXEV_LABELS, right = TRUE), levels = BS_MAXEV_LABELS)
}

# ── Shared theme ──────────────────────────────────────────────────────────────

bat_speed_chart_theme <- function() {
  ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.background    = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background   = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid.major.x = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
      panel.grid.minor.x = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor.y = ggplot2::element_blank(),
      strip.text  = ggplot2::element_text(face = "bold", size = 14),
      axis.title  = ggplot2::element_text(size = 13, face = "bold"),
      axis.text.x = ggplot2::element_text(size = 11, angle = 45, hjust = 1),
      axis.text.y = ggplot2::element_text(size = 12),
      plot.title  = ggplot2::element_text(face = "bold", size = 16),
      plot.margin = ggplot2::margin(12, 18, 12, 12)
    )
}

CARD_STYLE <- "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;"

# ── Module UI ─────────────────────────────────────────────────────────────────

batSpeedUI <- function(id) {
  ns <- NS(id)

  legend_html <- div(
    style = "display: flex; gap: 1.2rem; flex-wrap: wrap; font-size: 0.82rem; color: #555; margin-bottom: 1rem;",
    span(style = "display: inline-flex; align-items: center; gap: 4px;",
         tags$span(style = "font-size: 1.1rem; line-height: 1;", "◆"),
         "Mean"),
    span(style = "display: inline-flex; align-items: center; gap: 4px;",
         tags$span(style = "width: 14px; height: 2px; background: #333; display: inline-block;"),
         "Median (50th pctl)"),
    span(style = "display: inline-flex; align-items: center; gap: 4px;",
         tags$span(style = "width: 14px; height: 10px; border: 1.5px solid #2f7d3a; background: rgba(47,125,58,0.25); display: inline-block; border-radius: 2px;"),
         "25th-75th pctl")
  )

  div(
    class = "bs-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
       "Bat Speed, Exit Velocity & HR"),
    p(style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "Savant bat tracking leaderboard + Statcast BBE data, 2024-2025. Qualified hitters (502+ PA)."),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "Data & Methodology"),
      p("Bat speed data comes from Baseball Savant's bat tracking leaderboard (2024-2025). ",
        "Savant's reported bat speed is measured at the sweet spot and averaged over the fastest 90% of competitive swings. ",
        "Exit velocity metrics (Max EV, EV50, EV90) and HR totals were computed from individual ",
        "Statcast batted-ball events, matching the methodology in the HR-EV article. ",
        "Only qualified hitters (502+ PA) with bat tracking data are included (n = 260 player-seasons)."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "Correlations"),
      p(style = "margin-bottom: 0.5rem;",
        "Pearson correlations between average bat speed and each metric:"),
      tags$table(
        style = "max-width: 360px; border-collapse: collapse; font-size: 0.88rem; margin-bottom: 0.8rem;",
        tags$thead(
          tags$tr(style = "border-bottom: 2px solid #999;",
            tags$th(style = "text-align: left; padding: 0.35rem 0.5rem;", "Metric"),
            tags$th(style = "text-align: center; padding: 0.35rem 0.5rem;", "r"),
            tags$th(style = "text-align: center; padding: 0.35rem 0.5rem;", HTML("R<sup>2</sup>"))
          )
        ),
        tags$tbody(
          tags$tr(style = "border-bottom: 1px solid #ddd; font-weight: 600; background: #f0f7ef;",
            tags$td(style = "padding: 0.35rem 0.5rem;", "EV90"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.804"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.646")
          ),
          tags$tr(style = "border-bottom: 1px solid #ddd;",
            tags$td(style = "padding: 0.35rem 0.5rem;", "Max EV"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.696"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.485")
          ),
          tags$tr(style = "border-bottom: 1px solid #ddd;",
            tags$td(style = "padding: 0.35rem 0.5rem;", "EV50"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.681"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.464")
          ),
          tags$tr(
            tags$td(style = "padding: 0.35rem 0.5rem;", "HR/600 PA"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.500"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "0.250")
          )
        )
      ),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "Key Findings"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li("Bat speed correlates most strongly with EV90 (r=0.804) - the average exit velocity ",
                "of a hitter's hardest 10% of batted balls. This makes intuitive sense: ",
                "faster swings translate most directly to the quality of a hitter's best contact."),
        tags$li("The correlation with Max EV (r=0.696) is notably weaker than EV90, suggesting ",
                "that a single max-effort event has more noise than the top-10% average."),
        tags$li("Bat speed to HR rate is moderate (r=0.500, using HR/600 PA to normalize for playing time). ",
                "The same pattern from the HR-EV analysis holds: ",
                "raw power tools are necessary but not sufficient for HR production.")
      )
    ),

    # ── Section 1: Bat Speed by HR Bucket ────────────────────────────────────
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Bat Speed by HR/600 PA Bucket"),
    legend_html,
    div(style = CARD_STYLE, plotOutput(ns("chart_bs_by_hr"), height = "380px")),
    DTOutput(ns("table_bs_by_hr"), width = "100%"),

    tags$hr(style = "margin: 2.5rem 0; border-color: #ccc;"),

    # ── Section 2: Bat Speed by Max EV Bucket ────────────────────────────────
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Bat Speed by Max EV Bucket"),
    legend_html,
    div(style = CARD_STYLE, plotOutput(ns("chart_bs_by_ev"), height = "300px")),
    DTOutput(ns("table_bs_by_ev"), width = "100%"),

    tags$hr(style = "margin: 2.5rem 0; border-color: #ccc;"),

    # ── Section 3: Scatterplots ──────────────────────────────────────────────
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Bat Speed vs Exit Velocity & HR Rate"),
    div(style = CARD_STYLE, plotOutput(ns("scatter_ev90"), height = "360px")),
    div(style = CARD_STYLE, plotOutput(ns("scatter_maxev"), height = "360px")),
    div(style = CARD_STYLE, plotOutput(ns("scatter_ev50"), height = "360px")),
    div(style = CARD_STYLE, plotOutput(ns("scatter_hr600"), height = "360px")),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: Bat tracking leaderboard + Statcast BBE via Baseball Savant. ",
      "Bat speed = sweet-spot measurement, avg of top 90% competitive swings (Savant definition). ",
      "EV metrics computed from raw batted-ball events."
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

batSpeedServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    raw_data <- load_bat_speed_data()
    qual <- if (!is.null(raw_data)) {
      q <- raw_data[raw_data$pa >= 502, ]
      q$hr_per_600 <- q$hr / q$pa * 600
      q
    } else NULL

    # ── Bat Speed by HR/600 Bucket ────────────────────────────────────────────

    output$chart_bs_by_hr <- renderPlot({
      req(qual)
      d <- qual
      d$hr_bucket <- factor(assign_bs_hr600_bucket(d$hr_per_600), levels = rev(BS_HR600_LABELS))

      ggplot2::ggplot(d, ggplot2::aes(y = hr_bucket, x = avg_bat_speed)) +
        ggplot2::geom_boxplot(
          fill = "#2f7d3a", alpha = 0.25, colour = "#2f7d3a",
          outlier.size = 1, outlier.alpha = 0.15, lwd = 0.4
        ) +
        ggplot2::stat_summary(
          fun = mean, geom = "point", shape = 18, size = 3, colour = "#222"
        ) +
        ggplot2::scale_x_continuous(
          minor_breaks = function(lim) seq(floor(lim[1]), ceiling(lim[2]), by = 1)
        ) +
        ggplot2::labs(y = "HR/600 PA", x = "Bat Speed (mph)") +
        bat_speed_chart_theme()
    }, bg = "white")

    output$table_bs_by_hr <- renderDT({
      req(qual)
      d <- qual
      d$hr_bucket <- assign_bs_hr600_bucket(d$hr_per_600)

      summary_df <- data.frame(
        `HR/600 Bucket` = BS_HR600_LABELS,
        N = as.integer(table(d$hr_bucket)),
        `Avg HR/600` = tapply(d$hr_per_600, d$hr_bucket, function(v) round(mean(v), 1)),
        `Avg Bat Speed` = tapply(d$avg_bat_speed, d$hr_bucket, function(v) round(mean(v), 1)),
        check.names = FALSE, stringsAsFactors = FALSE, row.names = NULL
      )

      datatable(summary_df, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(summary_df),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })

    # ── Bat Speed by Max EV Bucket ────────────────────────────────────────────

    output$chart_bs_by_ev <- renderPlot({
      req(qual)
      d <- qual
      d$ev_bucket <- factor(assign_bs_maxev_bucket(d$max_ev), levels = rev(BS_MAXEV_LABELS))

      ggplot2::ggplot(d, ggplot2::aes(y = ev_bucket, x = avg_bat_speed)) +
        ggplot2::geom_boxplot(
          fill = "#b77343", alpha = 0.25, colour = "#b77343",
          outlier.size = 1, outlier.alpha = 0.15, lwd = 0.4
        ) +
        ggplot2::stat_summary(
          fun = mean, geom = "point", shape = 18, size = 3, colour = "#222"
        ) +
        ggplot2::scale_x_continuous(
          minor_breaks = function(lim) seq(floor(lim[1]), ceiling(lim[2]), by = 1)
        ) +
        ggplot2::labs(y = "Max EV", x = "Bat Speed (mph)") +
        bat_speed_chart_theme()
    }, bg = "white")

    output$table_bs_by_ev <- renderDT({
      req(qual)
      d <- qual
      d$ev_bucket <- assign_bs_maxev_bucket(d$max_ev)

      summary_df <- data.frame(
        `Max EV Bucket` = BS_MAXEV_LABELS,
        N = as.integer(table(d$ev_bucket)),
        `Avg Max EV` = tapply(d$max_ev, d$ev_bucket, function(v) round(mean(v), 1)),
        `Avg Bat Speed` = tapply(d$avg_bat_speed, d$ev_bucket, function(v) round(mean(v), 1)),
        check.names = FALSE, stringsAsFactors = FALSE, row.names = NULL
      )

      datatable(summary_df, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(summary_df),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })

    # ── Scatterplots ──────────────────────────────────────────────────────────

    make_scatter <- function(d, y_col, y_label, r_val) {
      ggplot2::ggplot(d, ggplot2::aes(x = avg_bat_speed, y = .data[[y_col]])) +
        ggplot2::geom_point(alpha = 0.4, size = 2, colour = "#2f7d3a") +
        ggplot2::geom_smooth(method = "lm", se = FALSE, colour = "#b77343", linewidth = 1) +
        ggplot2::annotate(
          "text", x = min(d$avg_bat_speed) + 0.5, y = max(d[[y_col]]) - 1,
          label = sprintf("r = %.3f", r_val),
          hjust = 0, size = 5, fontface = "bold", colour = "#333"
        ) +
        ggplot2::scale_x_continuous(
          minor_breaks = function(lim) seq(floor(lim[1]), ceiling(lim[2]), by = 1)
        ) +
        ggplot2::labs(x = "Bat Speed (mph)", y = y_label, title = paste("Bat Speed vs", y_label)) +
        bat_speed_chart_theme()
    }

    cors <- load_bat_speed_correlations()

    output$scatter_ev90 <- renderPlot({
      req(qual, cors)
      make_scatter(qual, "ev90", "EV90 (mph)", cors$r[cors$metric == "EV90"])
    }, bg = "white")

    output$scatter_maxev <- renderPlot({
      req(qual, cors)
      make_scatter(qual, "max_ev", "Max EV (mph)", cors$r[cors$metric == "Max EV"])
    }, bg = "white")

    output$scatter_ev50 <- renderPlot({
      req(qual, cors)
      make_scatter(qual, "ev50", "EV50 (mph)", cors$r[cors$metric == "EV50"])
    }, bg = "white")

    output$scatter_hr600 <- renderPlot({
      req(qual, cors)
      make_scatter(qual, "hr_per_600", "HR/600 PA", cors$r[cors$metric == "HR/600"])
    }, bg = "white")
  })
}
