# mod_research_hr_ev.R
# Research article: HR-Exit Velocity Relationship (2015-2025)

# ── Data loader ───────────────────────────────────────────────────────────────

load_hr_ev_player_data <- function() {
  path <- file.path("data", "processed", "hr_ev_research.csv")
  if (!file.exists(path)) return(NULL)
  dat <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  dat[dat$pa >= 502, , drop = FALSE]
}

# ── Bucket helpers ────────────────────────────────────────────────────────────

HR_BUCKET_LABELS <- c("<=10", "10-15", "15-20", "20-25", "25-30", "30-40", "40+")

assign_hr_bucket <- function(hr) {
  factor(
    cut(hr, breaks = c(-Inf, 10, 15, 20, 25, 30, 40, Inf),
        labels = HR_BUCKET_LABELS, right = TRUE),
    levels = HR_BUCKET_LABELS
  )
}

MAXEV_LABELS <- c("<=109", "110-112", "112-115", "115+")
EV90_LABELS  <- c("<=104", "104-106", "106-108", "108+")
EV50_LABELS  <- c("<=94", "94-96", "96-98", "98+")

assign_maxev_bucket <- function(x) {
  factor(cut(x, breaks = c(-Inf, 109, 112, 115, Inf),
             labels = MAXEV_LABELS, right = TRUE), levels = MAXEV_LABELS)
}
assign_ev90_bucket <- function(x) {
  factor(cut(x, breaks = c(-Inf, 104, 106, 108, Inf),
             labels = EV90_LABELS, right = TRUE), levels = EV90_LABELS)
}
assign_ev50_bucket <- function(x) {
  factor(cut(x, breaks = c(-Inf, 94, 96, 98, Inf),
             labels = EV50_LABELS, right = TRUE), levels = EV50_LABELS)
}

# ── Shared base theme ────────────────────────────────────────────────────────

hrev_base_theme <- function(bg = "#eef5ec") {
  ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.background    = ggplot2::element_rect(fill = bg, colour = NA),
      panel.background   = ggplot2::element_rect(fill = bg, colour = NA),
      panel.grid.major.x = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
      panel.grid.minor.x = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
      panel.grid.major.y = ggplot2::element_blank(),
      panel.grid.minor.y = ggplot2::element_blank(),
      strip.text  = ggplot2::element_text(face = "bold", size = 14),
      axis.title  = ggplot2::element_text(size = 13, face = "bold"),
      axis.text.x = ggplot2::element_text(size = 11, angle = 45, hjust = 1),
      axis.text.y = ggplot2::element_text(size = 12),
      plot.title  = ggplot2::element_text(face = "bold", size = 16),
      plot.margin = ggplot2::margin(10, 15, 10, 10)
    )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

hrEvUI <- function(id) {
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
    class = "hrev-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(
      style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
      "HR-Exit Velocity Relationship"
    ),
    p(
      style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "Statcast BBE data, 2015-2025 (excl. 2020). Qualified hitters (502+ PA). N = 1,378 player-seasons."
    ),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "Data & Methodology"),
      p("Individual batted-ball events (BBE) were pulled from Baseball Savant's Statcast search ",
        "for all regular-season plate appearances from 2015-2025 (excluding the shortened 2020 season). ",
        "Plate appearance counts and player names were sourced from the FanGraphs leaders API. ",
        "Only qualified hitters (502+ PA) are included."),
      p("Three exit-velocity metrics were computed per player-season from the raw BBE data:"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$strong("Max EV"), " - the single hardest-hit ball of the season."),
        tags$li(tags$strong("EV50"), " - average exit velocity of the hardest 50% of batted balls."),
        tags$li(tags$strong("EV90"), " - average exit velocity of the hardest 10% of batted balls.")
      ),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "Key Findings"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li("There is a clear, monotonic relationship between exit velocity and HR output ",
                "in both directions. Hitters in the 40+ HR bucket average ~7 mph higher Max EV ",
                "and ~5.5 mph higher EV50 than those in the <=10 HR bucket."),
        tags$li("The relationship is remarkably consistent across all three EV metrics, ",
                "suggesting that elite HR hitters don't just have one-off max-effort swings - ",
                "their entire contact-quality distribution is shifted higher."),
        tags$li("Variance within each bucket is fairly tight for EV (SD ~2-3 mph) but much wider ",
                "for HR counts (SD ~7-10 HR), reflecting that exit velocity is necessary but not ",
                "sufficient for HR production - launch angle, pull tendency, and playing time also matter.")
      ),

      # ── Quick-reference summary table ────────────────────────────────────
      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;", "At a Glance"),
      p(style = "font-size: 0.85rem; color: #555; margin-bottom: 0.5rem;",
        "Spread between the lowest and highest HR bucket for each metric:"),
      tags$table(
        style = "max-width: 420px; border-collapse: collapse; font-size: 0.88rem; margin-bottom: 0.8rem;",
        tags$thead(
          tags$tr(style = "border-bottom: 2px solid #999;",
            tags$th(style = "text-align: left; padding: 0.35rem 0.5rem;", "Metric"),
            tags$th(style = "text-align: center; padding: 0.35rem 0.5rem;", "<=10 HR"),
            tags$th(style = "text-align: center; padding: 0.35rem 0.5rem;", "40+ HR"),
            tags$th(style = "text-align: center; padding: 0.35rem 0.5rem;", "Gap")
          )
        ),
        tags$tbody(
          tags$tr(style = "border-bottom: 1px solid #ddd;",
            tags$td(style = "padding: 0.35rem 0.5rem;", "Max EV"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "109.0"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "115.9"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem; font-weight: 600;", "+6.9 mph")
          ),
          tags$tr(style = "border-bottom: 1px solid #ddd;",
            tags$td(style = "padding: 0.35rem 0.5rem;", "EV90"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "102.8"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "109.8"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem; font-weight: 600;", "+7.0 mph")
          ),
          tags$tr(
            tags$td(style = "padding: 0.35rem 0.5rem;", "EV50"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "93.7"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem;", "99.3"),
            tags$td(style = "text-align: center; padding: 0.35rem 0.5rem; font-weight: 600;", "+5.6 mph")
          )
        )
      ),
      p(style = "font-size: 0.85rem; color: #555;",
        "Roughly 6-7 mph of Max EV / EV90 separates a 10-HR hitter from a 40-HR hitter, ",
        "but only ~5.5 mph of EV50 - the top of the distribution separates more than the middle.")
    ),

    # ── Section 1: EV by HR bucket ───────────────────────────────────────────
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Exit Velocity by HR Bucket"),
    legend_html,

    div(style = "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;",
        plotOutput(ns("chart_ev_by_hr"), height = "440px")),
    DTOutput(ns("table_ev_by_hr"), width = "100%"),

    tags$hr(style = "margin: 2.5rem 0; border-color: #ccc;"),

    # ── Section 2: HR by EV bucket ───────────────────────────────────────────
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "HR by Exit Velocity Bucket"),
    legend_html,

    div(style = "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;",
        plotOutput(ns("chart_hr_ev50"), height = "280px")),
    div(style = "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;",
        plotOutput(ns("chart_hr_ev90"), height = "280px")),
    div(style = "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;",
        plotOutput(ns("chart_hr_maxev"), height = "280px")),
    DTOutput(ns("table_hr_by_ev"), width = "100%"),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: Statcast batted-ball events via Baseball Savant. ",
      "MaxEV = max exit velocity. ",
      "EV50 = mean EV of hardest 50% of batted balls. ",
      "EV90 = mean EV of hardest 10% of batted balls."
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

hrEvServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    player_data <- load_hr_ev_player_data()

    # ── Section 1: EV distributions by HR bucket ─────────────────────────────

    output$chart_ev_by_hr <- renderPlot({
      req(player_data)
      d <- player_data
      d$hr_bucket <- assign_hr_bucket(d$hr)

      long <- rbind(
        data.frame(hr_bucket = d$hr_bucket, metric = "Max EV", value = d$max_ev),
        data.frame(hr_bucket = d$hr_bucket, metric = "EV90",   value = d$ev90),
        data.frame(hr_bucket = d$hr_bucket, metric = "EV50",   value = d$ev50)
      )
      long$metric    <- factor(long$metric, levels = c("Max EV", "EV90", "EV50"))
      long$hr_bucket <- factor(long$hr_bucket, levels = rev(HR_BUCKET_LABELS))

      ggplot2::ggplot(long, ggplot2::aes(y = hr_bucket, x = value)) +
        ggplot2::geom_boxplot(
          fill = "#2f7d3a", alpha = 0.25, colour = "#2f7d3a",
          outlier.size = 1, outlier.alpha = 0.15,
          lwd = 0.4
        ) +
        ggplot2::stat_summary(
          fun = mean, geom = "point", shape = 18, size = 3, colour = "#222"
        ) +
        ggplot2::facet_wrap(~ metric, scales = "free_x", nrow = 1) +
        ggplot2::scale_x_continuous(minor_breaks = function(lim) {
          seq(floor(lim[1] / 2.5) * 2.5, ceiling(lim[2] / 2.5) * 2.5, by = 2.5)
        }) +
        ggplot2::labs(y = "HR Count", x = "Exit Velocity (mph)") +
        hrev_base_theme(bg = "white")
    }, bg = "white")

    output$table_ev_by_hr <- renderDT({
      req(player_data)
      d <- player_data
      d$hr_bucket <- assign_hr_bucket(d$hr)

      summary_df <- d |>
        (\(x) {
          data.frame(
            `HR Bucket` = levels(assign_hr_bucket(0)),
            N = as.integer(table(x$hr_bucket)),
            `Avg HR` = tapply(x$hr, x$hr_bucket, function(v) round(mean(v), 1)),
            `Max EV` = tapply(x$max_ev, x$hr_bucket, function(v) round(mean(v), 1)),
            EV90 = tapply(x$ev90, x$hr_bucket, function(v) round(mean(v), 1)),
            EV50 = tapply(x$ev50, x$hr_bucket, function(v) round(mean(v), 1)),
            check.names = FALSE, stringsAsFactors = FALSE, row.names = NULL
          )
        })()

      datatable(summary_df, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(summary_df),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })

    # ── Section 2: HR distributions by EV bucket (3 separate plots) ──────────

    make_hr_by_ev_plot <- function(d, bucket_fn, col_name, title, fill_col) {
      d$ev_bucket <- bucket_fn(d[[col_name]])
      d$ev_bucket <- factor(d$ev_bucket, levels = rev(levels(d$ev_bucket)))

      ggplot2::ggplot(d, ggplot2::aes(y = ev_bucket, x = hr)) +
        ggplot2::geom_boxplot(
          fill = fill_col, alpha = 0.25, colour = fill_col,
          outlier.size = 1, outlier.alpha = 0.15,
          lwd = 0.4
        ) +
        ggplot2::stat_summary(
          fun = mean, geom = "point", shape = 18, size = 3, colour = "#222"
        ) +
        ggplot2::scale_x_continuous(
          breaks = seq(0, 70, by = 10),
          minor_breaks = seq(0, 70, by = 5)
        ) +
        ggplot2::labs(y = "Exit Velocity", x = "Home Runs", title = title) +
        hrev_base_theme(bg = "white") +
        ggplot2::theme(
          plot.background  = ggplot2::element_rect(fill = "white", colour = "#ddd", linewidth = 0.5),
          panel.background = ggplot2::element_rect(fill = "white", colour = NA),
          plot.margin      = ggplot2::margin(12, 18, 12, 12)
        )
    }

    output$chart_hr_maxev <- renderPlot({
      req(player_data)
      make_hr_by_ev_plot(player_data, assign_maxev_bucket, "max_ev", "Max EV", "#b77343")
    }, bg = "#eef5ec")

    output$chart_hr_ev90 <- renderPlot({
      req(player_data)
      make_hr_by_ev_plot(player_data, assign_ev90_bucket, "ev90", "EV90", "#b77343")
    }, bg = "#eef5ec")

    output$chart_hr_ev50 <- renderPlot({
      req(player_data)
      make_hr_by_ev_plot(player_data, assign_ev50_bucket, "ev50", "EV50", "#b77343")
    }, bg = "#eef5ec")

    output$table_hr_by_ev <- renderDT({
      req(player_data)
      d <- player_data

      make_row <- function(bucket_fn, label) {
        col <- ifelse(label == "Max EV", "max_ev", ifelse(label == "EV90", "ev90", "ev50"))
        b <- bucket_fn(d[[col]])
        data.frame(
          Metric = label,
          Bucket = levels(b),
          N = as.integer(table(b)),
          `Avg HR` = tapply(d$hr, b, function(v) round(mean(v), 1)),
          check.names = FALSE, stringsAsFactors = FALSE, row.names = NULL
        )
      }

      summary_df <- rbind(
        make_row(assign_ev50_bucket,  "EV50"),
        make_row(assign_ev90_bucket,  "EV90"),
        make_row(assign_maxev_bucket, "Max EV")
      )

      datatable(summary_df, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(summary_df),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })
  })
}
