# mod_research_adj_barrel.R
# Research article: Season-Adjusted Barrels (aBrl)
# Recalibrates the barrel zone per-season to maintain consistent outcome quality.

# ── Data loaders ─────────────────────────────────────────────────────────────

load_abrl_seasons <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_seasons.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

load_abrl_hr_rates <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_hr_rates.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

load_abrl_hitters <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_hitters.csv")
  if (!file.exists(path)) return(NULL)
  d <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  if ("player_name" %in% names(d)) {
    Encoding(d$player_name) <- "UTF-8"
  }
  d
}

load_abrl_convergence <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_convergence.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

load_abrl_stabilization <- function() {
  path <- file.path("data", "processed", "adjusted_barrel_stabilization.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── Chart theme ──────────────────────────────────────────────────────────────

abrl_theme <- function() {
  ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.background    = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background   = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid.major   = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
      panel.grid.minor   = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
      axis.title  = ggplot2::element_text(size = 13, face = "bold"),
      axis.text   = ggplot2::element_text(size = 11),
      plot.title  = ggplot2::element_text(face = "bold", size = 15),
      plot.subtitle = ggplot2::element_text(size = 11, color = "#555"),
      plot.margin = ggplot2::margin(12, 18, 12, 12)
    )
}

ABRL_CARD <- "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;"
ABRL_GREEN <- "#2f7d3a"
ABRL_BROWN <- "#8b5e3c"

# ── Module UI ────────────────────────────────────────────────────────────────

adjBarrelUI <- function(id) {
  ns <- NS(id)

  div(
    class = "abrl-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
       "Season-Adjusted Barrels (aBrl)"),
    p(style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "Recalibrating the barrel zone to maintain consistent outcome quality, 2015-2025."),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      # ── Background ──
      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Background"),
      p("In 2016, Tom Tango introduced the ",
        tags$a(href = "https://tangotiger.com/index.php/site/comments/statcast-lab-barrels",
               target = "_blank", style = "color: #2f7d3a;",
               "barrel"),
        " as a batted ball with exit velocity and launch angle in the sweet spot that ",
        "historically produced elite outcomes (minimum of a .500 BA and a 1.500 SLG). ",
        "The average barrel in 2015-2016 actually hit for a .808 AVG + 2.795 SLG. ",
        "The definition was calibrated on 2015-2016 Statcast data using four boundary conditions:"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem; font-family: monospace; font-size: 0.85rem;",
        tags$li(HTML("EV &ge; 98 mph")),
        tags$li(HTML("Launch angle between 4&deg; and 50&deg;")),
        tags$li(HTML("EV &times; 1.5 &minus; LA &ge; 117")),
        tags$li(HTML("EV + LA &ge; 124"))
      ),
      p("Unlike metrics such as ",
        tags$a(href = "https://www.mlb.com/glossary/statcast/expected-woba",
               target = "_blank", style = "color: #2f7d3a;",
               "xwOBA"),
        " (retrained on cumulative Statcast data), ",
        "Stuff+ (fully retrained each offseason per Eno Sarris), or ",
        "wRC+ park factors (rolling 5-year window based on Savant Park Factors), ",
        tags$b("the barrel definition has never been updated."),
        " It remains frozen to the 2015-2016 calibration."),

      # ── The Problem ──
      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "The Problem: Barrel Outcomes Are Drifting"),
      p("As ",
        tags$a(href = "https://blogs.fangraphs.com/they-dont-make-barrels-like-they-used-to/",
               target = "_blank", style = "color: #2f7d3a;",
               "Ben Clemens of FanGraphs"),
        " documented, wOBA on non-HR barrels has fallen from .656 to .508 from 2015-2025. ",
        "I confirmed this trend in my ",
        tags$a(href = "#research_park_hr_barrel",
               style = "color: #2f7d3a;",
               "Park-Adjusting HR/Barrel Rate"),
        " article, where I observed that HR/Barrel rate has also declined dramatically ",
        "over the past decade from roughly 59% in 2015 to 48% in 2025. ",
        "A barrel in 2025 is defined by the same results-oriented criteria as in 2015, ",
        "but the batted balls meeting that original definition produce significantly less value."),
      p("The causes are likely multifactorial. Changes in ball composition ",
        "(the periodic deadening and re-juicing of baseballs) directly affect how far barrels travel. ",
        "Defensive positioning has evolved ", HTML("&mdash;"), " outfielders play deeper and shift more aggressively against ",
        "pull-heavy hitters, turning some barrel outcomes into outs. And there may be a ",
        tags$a(href = "https://en.wikipedia.org/wiki/Goodhart%27s_law",
               target = "_blank", style = "color: #2f7d3a;",
               "Goodhart's Law"),
        " effect: as barrel rate became a mainstream metric, hitters optimized for it, ",
        "increasing the supply of barrel-qualifying contact while diluting its average quality. ",
        "Whatever the mix of causes, the result is the same: the barrel zone no longer means what it ",
        "once did."),

      # ── Tango's Invitation ──
      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Tango's Invitation"),
      p("In a 2018 comment on his original barrel blog post, Tango acknowledged this drift: ",
        tags$span(style = "font-style: italic; background: #f5f5f0; padding: 2px 6px; border-radius: 3px;",
                  HTML("&ldquo;This was based on 2016 data. 2017 is a bit different, ",
                  "so if someone wants to tweak these, by all means.&rdquo;"))),
      p("Nobody has systematically taken him up on that offer -- until now."),

      # ── Methodology ──
      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Methodology: Adjusted Barrels (aBrl)"),
      p("This approach recalibrates the barrel zone each season by identifying an EV shift ",
        "that maintains the same barrel outcome quality as Tango's 2015-2016 calibration ",
        "(mean .808 AVG / 2.795 SLG on barrel events, min .500 AVG / 1.500 SLG). ",
        "The four barrel inequalities stay the same, I just moved the barrel zone along the EV axis:"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(HTML("For each season, find the EV shift (&Delta;) that keeps barrel outcome quality ",
                "matching the 2015-2016 calibration")),
        tags$li(HTML("Positive &Delta; = looser threshold (balls produce better outcomes &rarr; lower EV needed)")),
        tags$li(HTML("Negative &Delta; = stricter threshold (balls produce worse outcomes &rarr; higher EV needed)"))
      ),
      p(HTML("I verified this reproduces Tango's original zone on 2015-2016 data (&Delta; &asymp; 0). ",
        "I also tested a 2-parameter approach (shifting both EV and LA) but found the ",
        "LA dimension worsened results.")),

      # ── Naming ──
      p(style = "margin-top: 0.8rem;",
        "I call this adjusted version of a barrel.....  ",
        tags$b("aBrl"), " (adjusted Barrel).")
    ),

    # ── Section 1: EV Threshold Chart + Table ─────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Season-Adjusted EV Thresholds"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "The EV floor required to produce barrel-quality outcomes has risen from 98.1 to 100.3 mph. ",
      "Notice how Tango barrel AVG/SLG drifts downward while aBrl AVG/SLG stays flatter."),
    div(style = ABRL_CARD, plotOutput(ns("ev_threshold_chart"), height = "380px")),
    div(style = ABRL_CARD, DTOutput(ns("ev_table"), width = "100%")),

    # ── Section 3: HR/aBrl Stability ─────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Validation: HR/aBrl Is Stable"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "The strongest validation in my mind is that HR/aBrl is nearly flat across the entire decade ",
      "unlike the original HR/Brl. HR/Brl SD = 6.08, HR/aBrl SD = 0.87"),
    div(style = ABRL_CARD, plotOutput(ns("hr_brl_stable_chart"), height = "380px")),

    # ── Section 4: 2025 Leaderboard ──────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "2025 Barrel Rate: Tango vs aBrl"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 0.5rem;",
      "With the 2025 EV floor at 100.3 mph, every hitter loses barrels under the adjusted definition. ",
      "Hitters with marginal exit velocities near the old 98 mph boundary lose the most barrels, ",
      "and the leaderboard reshuffles significantly ", HTML("&mdash;"), " 56% of qualified hitters move 10 or more spots ",
      "in the aBrl% leaderboard compared to Tango Brl%."),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 0.5rem;",
      "Showing the top 25 qualified hitters (min 200 BBE). "),
    div(
      style = "margin-bottom: 1rem; padding: 10px 14px; background: #f0f7f0; border: 1px solid #c9d7c5; border-radius: 8px;",
      p(style = "font-size: 0.88rem; color: #333; margin: 0;",
        tags$b("Full leaderboard: "),
        "Year-by-year Brl% vs aBrl% for all qualified hitters is available in the ",
        tags$a(href = "https://docs.google.com/spreadsheets/d/16WxkFwZYKcurMM0CJJr4l5mo1S6rs-yKnnr1ZfmoGjU/edit",
               target = "_blank", style = "color: #2f7d3a; font-weight: 600;",
               "companion spreadsheet"),
        " and the ",
        tags$a(href = "#abrl_leaderboard",
               style = "color: #2f7d3a; font-weight: 600;",
               "interactive leaderboard"),
        ".")
    ),
    div(
      style = "margin-bottom: 0.8rem; font-size: 0.84rem; color: #555; line-height: 1.5;",
      p(style = "margin: 0 0 0.3rem 0;",
        tags$b("Diff"), " = aBrl% minus Brl%, the change in barrel rate as a % of total BBE. ",
        tags$b("% Lost"), " = the fraction of a hitter's Tango barrels that fail the aBrl definition."),
      p(style = "margin: 0;",
        "% Lost captures hitters most vulnerable to this change -- those whose barrel profile sits ",
        "near the original 98 mph boundary. Diff captures whose ranking actually moves the most on a leaderboard.")
    ),
    radioButtons(ns("leaderboard_view"), NULL,
                 choices = c("Biggest Movers" = "movers",
                             "aBrl% Leaders" = "leaders",
                             "Tango Brl% Leaders" = "tango"),
                 selected = "movers", inline = TRUE),
    div(style = ABRL_CARD, DTOutput(ns("leaderboard_table"), width = "100%")),

    # ── Section 6: Convergence ───────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "In-Season Convergence"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 0.5rem;",
      "To apply aBrl during a season, I need enough batted ball data to calibrate that year's ",
      "EV shift. The chart below shows the estimation error: if I run the calibration using only ",
      "the first N games of data, how far off is the EV shift from the final end-of-season value?"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "At 15 games in, the estimate is off by about 1 mph. By ~115 games (mid-August), ",
      "the error drops below 0.2 mph -- accurate enough for practical use. ",
      "At 162 games, the error is zero by definition (that is the full-season calibration)."),
    div(style = ABRL_CARD, plotOutput(ns("convergence_chart"), height = "340px")),

    # ── Section 7: Predictive Power ──────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Predictive Power: Barrel% vs aBrl%"),

    # Table
    div(style = ABRL_CARD,
      tags$table(
        style = "width: 100%; border-collapse: collapse; font-size: 0.88rem; text-align: center;",
        tags$thead(
          tags$tr(style = "border-bottom: 2px solid #ccc;",
            tags$th(style = "text-align: left; padding: 6px 10px;", ""),
            tags$th(style = "padding: 6px 10px;", "Barrel%"),
            tags$th(style = "padding: 6px 10px;", "aBrl%")
          )
        ),
        tags$tbody(
          tags$tr(style = "border-bottom: 1px solid #eee;",
            tags$td(style = "text-align: left; padding: 6px 10px; font-weight: 600;", HTML("Same-season r &rarr; HR")),
            tags$td(style = "padding: 6px 10px;", ".859"),
            tags$td(style = "padding: 6px 10px;", ".858")
          ),
          tags$tr(style = "border-bottom: 1px solid #eee;",
            tags$td(style = "text-align: left; padding: 6px 10px; font-weight: 600;", HTML("Same-season r &rarr; SLG")),
            tags$td(style = "padding: 6px 10px;", ".703"),
            tags$td(style = "padding: 6px 10px;", ".705")
          ),
          tags$tr(style = "border-bottom: 1px solid #eee;",
            tags$td(style = "text-align: left; padding: 6px 10px; font-weight: 600;", HTML("Next-season r &rarr; HR")),
            tags$td(style = "padding: 6px 10px;", ".665"),
            tags$td(style = "padding: 6px 10px;", ".663")
          ),
          tags$tr(
            tags$td(style = "text-align: left; padding: 6px 10px; font-weight: 600;", HTML("Next-season r &rarr; SLG")),
            tags$td(style = "padding: 6px 10px;", ".508"),
            tags$td(style = "padding: 6px 10px;", ".506")
          )
        )
      ),
      p(style = "font-size: 0.78rem; color: #888; margin-top: 8px; margin-bottom: 0;",
        HTML("Mean correlation across 2015&ndash;2025 (excl. 2020), min 400 PA. HR scaled to 600 PA."))
    ),

    # Writeup
    p(style = "font-size: 0.88rem; color: #555; margin-top: 1rem; margin-bottom: 0.5rem;",
      "While the leaderboard does reshuffle at the individual level (as shown above), ",
      "the aggregate changes are much more diffuse. The highest barrel rate hitters are still the ",
      "highest aBrl rate hitters for the most part, which is why the aggregate predictiveness ",
      "of both Brl% and aBrl% are so similar. Being 2nd vs 10th in barrel rate is meaningful at the ",
      "level of individual analysis, but for predicting power at the league level, high barrel rate = ",
      "good for both barrel and aBrl."),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "Where I am proposing that aBrl adds the most value is in making the barrel a stable unit of ",
      "measurement. Under the original definition, barrel SLG has drifted from 2.78 to 2.34 over the ",
      "past decade, and the HR/barrel rate has fallen from ~59% to ~48%. aBrl recalibrates the threshold ",
      "each season so that a barrel always means the same quality of contact. This means that a player ",
      "with an 8% aBrl rate in 2025 represents the same results-oriented quality of contact as an 8% ",
      "aBrl rate in 2016, unlike the static barrel definition. ",
      "The primary beneficiaries of this adjustment are models and projection systems that use barrel ",
      "rate as an input feature, where a stable relationship between barrels and outcomes prevents ",
      "silent coefficient drift over time."),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: Statcast BBE data via Baseball Savant, 2015-2025 (excl. 2020). ",
      "Calibration target: 2015-2016 barrel outcome quality (.808 AVG / 2.795 SLG). ",
      "Regular season only. ",
      "Methodology by @Collinmyshot."
    )
  )
}

# ── Module Server ────────────────────────────────────────────────────────────

adjBarrelServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    seasons_data  <- load_abrl_seasons()
    hr_rates      <- load_abrl_hr_rates()
    hitters_data  <- load_abrl_hitters()
    conv_data     <- load_abrl_convergence()

    # ── Chart 1: EV Threshold + Barrel AVG/SLG (dual axis) ──────────────────

    output$ev_threshold_chart <- renderPlot({
      req(seasons_data)
      d <- seasons_data

      # Scale parameters for dual axis: map SLG (2.2-3.0) to EV floor range
      ev_min <- 96; ev_max <- 102
      slg_min <- 2.2; slg_max <- 3.0
      scale_to_ev <- function(val) ev_min + (val - slg_min) / (slg_max - slg_min) * (ev_max - ev_min)

      col_ev <- "#333333"
      col_slg <- "#2980b9"

      ggplot2::ggplot(d, ggplot2::aes(x = season)) +
        # EV Floor (left axis)
        ggplot2::geom_line(ggplot2::aes(y = ev_floor), colour = col_ev, linewidth = 1.3) +
        ggplot2::geom_point(ggplot2::aes(y = ev_floor), colour = col_ev, size = 3) +
        ggplot2::geom_text(ggplot2::aes(y = ev_floor, label = sprintf("%.1f", ev_floor)),
                           vjust = -1.3, size = 3, colour = col_ev) +
        # Brl SLG (right axis, scaled)
        ggplot2::geom_line(ggplot2::aes(y = scale_to_ev(tango_brl_slg)), colour = col_slg,
                           linewidth = 1.1, linetype = "dashed") +
        ggplot2::geom_point(ggplot2::aes(y = scale_to_ev(tango_brl_slg)), colour = col_slg, size = 2.5) +
        ggplot2::geom_text(ggplot2::aes(y = scale_to_ev(tango_brl_slg),
                                         label = sprintf("%.3f", tango_brl_slg)),
                           vjust = -1.3, size = 2.8, colour = col_slg) +
        # Scales
        ggplot2::scale_x_continuous(breaks = d$season) +
        ggplot2::scale_y_continuous(
          name = "EV Floor (mph)",
          limits = c(ev_min, ev_max),
          breaks = seq(ev_min, ev_max, by = 1),
          sec.axis = ggplot2::sec_axis(
            ~ slg_min + (. - ev_min) / (ev_max - ev_min) * (slg_max - slg_min),
            name = "Barrel SLG",
            breaks = seq(slg_min, slg_max, by = 0.2),
            labels = function(x) sprintf("%.1f", x)
          )
        ) +
        ggplot2::labs(
          x = "Season",
          title = "EV Floor vs Tango Barrel Outcome Quality",
          subtitle = "As the EV floor rises, Tango barrel SLG drifts downward"
        ) +
        abrl_theme() +
        ggplot2::theme(
          legend.position = "none",
          axis.text.x = ggplot2::element_text(angle = 45, hjust = 1),
          axis.title.y.left = ggplot2::element_text(colour = col_ev),
          axis.text.y.left = ggplot2::element_text(colour = col_ev),
          axis.title.y.right = ggplot2::element_text(colour = col_slg),
          axis.text.y.right = ggplot2::element_text(colour = col_slg)
        ) +
        # Manual legend in top-right corner (away from data)
        ggplot2::annotate("text", x = max(d$season) - 0.3, y = ev_min + 0.5,
                          label = "EV Floor", colour = col_ev, size = 3.2, hjust = 1, fontface = "bold") +
        ggplot2::annotate("text", x = max(d$season) - 0.3, y = ev_min + 0.15,
                          label = "Brl SLG", colour = col_slg, size = 3.2, hjust = 1, fontface = "bold")
    }, res = 96)

    # ── Table 1: EV Thresholds ─────────────────────────────────────────────

    output$ev_table <- DT::renderDT({
      req(seasons_data)
      d <- seasons_data |>
        dplyr::mutate(
          `Season` = season,
          `EV Shift` = sprintf("%+.2f", ev_shift),
          `EV Floor` = sprintf("%.1f", ev_floor),
          `Tango Brls` = format(tango_n, big.mark = ","),
          `aBrls` = format(adj_n, big.mark = ","),
          `Brl AVG` = sprintf("%.3f", tango_brl_avg),
          `aBrl AVG` = sprintf("%.3f", adj_brl_avg),
          `Brl SLG` = sprintf("%.3f", tango_brl_slg),
          `aBrl SLG` = sprintf("%.3f", adj_brl_slg)
        ) |>
        dplyr::select(`Season`, `EV Shift`, `EV Floor`,
                       `Tango Brls`, `aBrls`,
                       `Brl AVG`, `aBrl AVG`, `Brl SLG`, `aBrl SLG`)

      DT::datatable(
        d, rownames = FALSE,
        options = list(dom = "t", paging = FALSE, ordering = FALSE,
                       columnDefs = list(
                         list(className = "dt-center", targets = "_all")
                       )),
        class = "compact stripe"
      )
    })

    # ── Chart 3: HR/aBrl Stability ─────────────────────────────────────────

    output$hr_brl_stable_chart <- renderPlot({
      req(hr_rates)
      d <- hr_rates

      ggplot2::ggplot(d, ggplot2::aes(x = season)) +
        ggplot2::geom_line(ggplot2::aes(y = tango_hr_brl_pct, colour = "Tango Brl"),
                           linewidth = 1.2) +
        ggplot2::geom_point(ggplot2::aes(y = tango_hr_brl_pct, colour = "Tango Brl"),
                            size = 3) +
        ggplot2::geom_line(ggplot2::aes(y = adj_hr_brl_pct, colour = "aBrl"),
                           linewidth = 1.2) +
        ggplot2::geom_point(ggplot2::aes(y = adj_hr_brl_pct, colour = "aBrl"),
                            size = 3) +
        ggplot2::geom_text(ggplot2::aes(y = tango_hr_brl_pct,
                                         label = sprintf("%.1f%%", tango_hr_brl_pct)),
                           vjust = 2, size = 3, colour = ABRL_BROWN) +
        ggplot2::geom_text(ggplot2::aes(y = adj_hr_brl_pct,
                                         label = sprintf("%.1f%%", adj_hr_brl_pct)),
                           vjust = -1.2, size = 3, colour = ABRL_GREEN) +
        ggplot2::scale_colour_manual(
          values = c("Tango Brl" = ABRL_BROWN, "aBrl" = ABRL_GREEN),
          name = NULL
        ) +
        ggplot2::scale_x_continuous(breaks = d$season) +
        ggplot2::scale_y_continuous(limits = c(40, 72),
                                    labels = function(x) paste0(x, "%")) +
        ggplot2::labs(
          x = "Season", y = "HR / Barrel %",
          title = "HR per Barrel: Tango vs aBrl",
          subtitle = "aBrl maintains ~60% HR rate across the entire decade (SD = 0.87 vs 6.08)"
        ) +
        abrl_theme() +
        ggplot2::theme(
          legend.position = "top",
          axis.text.x = ggplot2::element_text(angle = 45, hjust = 1)
        )
    }, res = 96)

    # ── Table 4: 2025 Leaderboard ──────────────────────────────────────────

    output$leaderboard_table <- DT::renderDT({
      req(hitters_data)
      view <- input$leaderboard_view

      d <- hitters_data |>
        dplyr::filter(season == 2025, total_bbe >= 200) |>
        dplyr::mutate(
          brl_diff_pct = round(adj_brl_pct - tango_brl_pct, 1)
        )

      if (view == "movers") {
        d <- d |> dplyr::arrange(brl_diff_pct)
      } else if (view == "leaders") {
        d <- d |> dplyr::arrange(dplyr::desc(adj_brl_pct))
      } else {
        d <- d |> dplyr::arrange(dplyr::desc(tango_brl_pct))
      }

      d <- d |>
        dplyr::slice_head(n = 25) |>
        dplyr::mutate(
          `#` = dplyr::row_number(),
          Player = ifelse(is.na(player_name), as.character(batter), player_name),
          BBE = total_bbe,
          `Brl%` = sprintf("%.1f%%", tango_brl_pct),
          `aBrl%` = sprintf("%.1f%%", adj_brl_pct),
          `Diff` = sprintf("%+.1f", brl_diff_pct),
          `Brl EV` = ifelse(is.na(barrel_ev_mean), "--", sprintf("%.1f", barrel_ev_mean)),
          `% Lost` = ifelse(is.na(pct_lost), "--", sprintf("%.0f%%", pct_lost))
        ) |>
        dplyr::select(`#`, Player, BBE, `Brl%`, `aBrl%`, Diff, `Brl EV`, `% Lost`)

      DT::datatable(
        d, rownames = FALSE,
        options = list(dom = "t", paging = FALSE, ordering = TRUE,
                       columnDefs = list(
                         list(className = "dt-center", targets = c(0, 2:7)),
                         list(className = "dt-left", targets = 1),
                         list(width = "30px", targets = 0)
                       )),
        class = "compact stripe"
      ) |>
        DT::formatStyle("Diff",
                         color = DT::styleInterval(
                           c(-3, -0.5, 0.5), c("#b74343", "#c97a3a", "#555", "#2f7d3a")
                         ))
    })

    # ── Chart: Convergence ──────────────────────────────────────────────────

    output$convergence_chart <- renderPlot({
      req(conv_data)

      d <- conv_data |>
        dplyr::group_by(fraction) |>
        dplyr::summarise(
          avg_n_bbe = mean(n_bbe),
          mean_abs_error = mean(abs(error), na.rm = TRUE),
          .groups = "drop"
        ) |>
        dplyr::mutate(
          games = round(fraction * 162)
        )

      # Add the 100% point (error = 0 by definition)
      d <- dplyr::bind_rows(d, dplyr::tibble(
        fraction = 1.0, avg_n_bbe = max(d$avg_n_bbe) / 0.9,
        mean_abs_error = 0, games = 162L
      ))

      ggplot2::ggplot(d, ggplot2::aes(x = games, y = mean_abs_error)) +
        ggplot2::geom_area(fill = ABRL_GREEN, alpha = 0.15) +
        ggplot2::geom_line(colour = ABRL_GREEN, linewidth = 1.2) +
        ggplot2::geom_point(colour = ABRL_GREEN, size = 3) +
        ggplot2::geom_text(ggplot2::aes(
          label = sprintf("%.2f", mean_abs_error)),
          vjust = -1.3, size = 3, colour = "#555") +
        ggplot2::scale_x_continuous(
          breaks = c(15, 50, 80, 115, 162),
          limits = c(0, 175)
        ) +
        ggplot2::scale_y_continuous(
          labels = function(x) sprintf("%.2f", x)
        ) +
        ggplot2::labs(
          x = "Games Played",
          y = "EV Shift Error (mph)",
          title = "In-Season Parameter Convergence",
          subtitle = "How quickly the season's aBrl EV shift converges to its final value"
        ) +
        abrl_theme()
    }, res = 96)

  })
}
