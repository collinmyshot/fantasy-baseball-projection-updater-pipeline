# mod_research_hitter_whiff.R
# Research article: Predicting Hitter Strikeouts — Whiff% vs SwStr% vs CSW%
# Companion to the pitcher article. Savant Whiff% + FanGraphs, 2015-2025 (excl. 2020)

# ── Data loader ──────────────────────────────────────────────────────────────

load_hitter_whiff_raw <- function() {
  path <- file.path("data", "processed", "hitter_whiff_research_raw.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── Chart theme ──────────────────────────────────────────────────────────────

hitter_chart_theme <- function() {
  ggplot2::theme_minimal(base_size = 15) +
    ggplot2::theme(
      plot.background    = ggplot2::element_rect(fill = "white", colour = NA),
      panel.background   = ggplot2::element_rect(fill = "white", colour = NA),
      panel.grid.major   = ggplot2::element_line(colour = "#ddd", linewidth = 0.3),
      panel.grid.minor   = ggplot2::element_line(colour = "#eee", linewidth = 0.2),
      axis.title  = ggplot2::element_text(size = 13, face = "bold"),
      axis.text   = ggplot2::element_text(size = 11),
      plot.title  = ggplot2::element_text(face = "bold", size = 15),
      plot.margin = ggplot2::margin(12, 18, 12, 12),
      legend.position = "top",
      legend.text = ggplot2::element_text(size = 11)
    )
}

HW_CARD <- "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;"

# ── Helper: styled table ─────────────────────────────────────────────────────

hw_table_html <- function(rows, col_headers, highlight_row = NULL, caption = NULL) {
  header_cells <- paste(
    sapply(col_headers, function(h) {
      sprintf('<th style="text-align: center; padding: 0.35rem 0.6rem;">%s</th>', h)
    }),
    collapse = ""
  )
  header_cells <- sub('text-align: center;', 'text-align: left;', header_cells, fixed = TRUE)

  body_rows <- sapply(seq_len(nrow(rows)), function(i) {
    bg <- if (!is.null(highlight_row) && i == highlight_row) {
      " font-weight: 600; background: #f0f7ef;"
    } else ""
    cells <- paste(
      sapply(seq_len(ncol(rows)), function(j) {
        align <- if (j == 1) "left" else "center"
        sprintf('<td style="text-align: %s; padding: 0.35rem 0.6rem;">%s</td>', align, rows[i, j])
      }),
      collapse = ""
    )
    sprintf('<tr style="border-bottom: 1px solid #ddd;%s">%s</tr>', bg, cells)
  })

  caption_html <- if (!is.null(caption)) {
    sprintf('<div style="font-size: 0.82rem; font-weight: 600; color: #444; margin-bottom: 0.4rem;">%s</div>', caption)
  } else ""

  sprintf(
    paste0(
      '%s',
      '<table style="width: 100%%; border-collapse: collapse; font-size: 0.88rem; margin-bottom: 0.8rem;">',
      '<thead><tr style="border-bottom: 2px solid #999;">%s</tr></thead>',
      '<tbody>%s</tbody></table>'
    ),
    caption_html, header_cells, paste(body_rows, collapse = "")
  )
}

# ── Module UI ────────────────────────────────────────────────────────────────

hitterWhiffUI <- function(id) {
  ns <- NS(id)

  div(
    class = "hitter-whiff-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
       "Predicting Hitter Strikeouts"),
    p(style = "font-size: 1.05rem; color: #555; margin-bottom: 0.2rem;",
      "The Batter's Perspective: Does Whiff% Predict K% the Same Way?"),
    p(style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "FanGraphs + Baseball Savant, 2015-2025 (excl. 2020). Hitters with 200+ PA."),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Context"),
      p("In the ", tags$a(href = "#", onclick = "Shiny.setInputValue('nav_to_pitcher', Math.random())",
                          style = "color: #4a6fa5; text-decoration: underline;",
                          "companion pitcher article"),
        ", we found that Whiff% (from Savant) is the best predictor of pitcher K% — it stabilizes ",
        "faster than K% itself, converts nearly 1:1, and dominates SwStr% and CSW% at every horizon. ",
        "But does that hold from the batter's side?"),
      p("The short answer: ", tags$b("yes, and it's even stronger."), " Hitter Whiff% explains ",
        "79% of K% variance same-season (vs 76% for pitchers) and predicts next-year K% at r=0.767 ",
        "(vs 0.665 for pitchers). Batters are simply more consistent creatures than pitchers — ",
        "their plate discipline profiles are stickier year-to-year."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Key Findings"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$b("Whiff% dominates even more:"), " Same-season r=0.891 (vs SwStr% 0.767, CSW% 0.744). ",
                "The gap between Whiff% and SwStr% is much larger for hitters than pitchers."),
        tags$li(tags$b("93% as predictive as K% itself:"), " Year-over-year, hitter Whiff% predicts next-year K% ",
                "at r=0.767 while K% predicts itself at r=0.821. For pitchers this ratio was 91%."),
        tags$li(tags$b("Stabilizes faster than K%:"), " Split-half reliability for Whiff% is r=0.837 at 100 PA, ",
                "while K% only reaches r=0.765. Just like pitchers, Whiff% stabilizes before the thing it predicts."),
        tags$li(tags$b("Conversion: K% ≈ Whiff% - 3pp:"), " Slope of 0.873, near-zero intercept. ",
                "Mean Whiff% = 24.5%, mean K% = 21.8%. Slightly more spread than pitchers (~2pp offset) ",
                "but still trivially easy to convert."),
        tags$li(tags$b("SwStr% is less useful for hitters:"), " The 2× shortcut (K% ≈ 2 × SwStr%) is weaker — ",
                "the actual ratio is 2.03× but R² is only 0.589 (vs 0.703 for pitchers). ",
                "The slope is just 1.43 with a positive intercept of +6.5%, making the simple multiplier ",
                "less reliable. Whiff% is the clear winner for hitter K% work.")
      ),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Why Is Everything More Stable for Hitters?"),
      p("Hitters face 600+ PA per season and their approach is largely self-determined — ",
        "swing decisions are a core skill that doesn't depend on opposing lineups the way ",
        "pitcher matchups vary game-to-game. A hitter's contact ability (or lack thereof) ",
        "is one of the stickiest traits in baseball, which is why Whiff% YoY self-correlation ",
        "is r=0.865 for hitters vs 0.741 for pitchers.")
    ),

    # ── Section 1: Same-Season ───────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Same-Season Correlations with K%"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1.5rem;",
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
        HTML(hw_table_html(
          data.frame(
            Predictor = c("Whiff%", "SwStr%", "CSW%"),
            r         = c("0.891", "0.767", "0.744"),
            `R²`  = c("0.794", "0.589", "0.554"),
            check.names = FALSE
          ),
          c("Predictor", "r", "R²"),
          highlight_row = 1,
          caption = "Hitters (n = 3,564, 200+ PA)"
        )),
        p(style = "font-size: 0.82rem; color: #666; margin-top: 0.3rem; margin-bottom: 0;",
          "Compare: Pitcher Whiff% → K% was r=0.872. The hitter relationship is even tighter.")
      ),
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
          plotOutput(ns("scatter_whiff_k"), height = "300px"))
    ),

    # ── Section 2: Predictiveness ────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Predictiveness"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1.5rem;",
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
        HTML(hw_table_html(
          data.frame(
            `1H Metric` = c("Whiff%", "SwStr%", "CSW%", "K%"),
            `2H K%`     = c("0.735", "0.633", "0.575", "0.765"),
            check.names = FALSE
          ),
          c("1H Metric", "r → 2H K%"),
          highlight_row = 1,
          caption = "1H → 2H (100+ PA each half, n = 2,800)"
        ))
      ),
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
        HTML(hw_table_html(
          data.frame(
            `Year N` = c("Whiff%", "SwStr%", "CSW%", "K%"),
            `Year N+1 K%` = c("0.767", "0.649", "0.611", "0.821"),
            check.names = FALSE
          ),
          c("Year N", "r → Year N+1 K%"),
          highlight_row = 1,
          caption = "Year-over-year (200+ PA both, n = 2,142)"
        ))
      )
    ),

    # ── Section 3: Stabilization ─────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Stabilization"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "Split-half reliability (1H vs 2H) at 100+ PA per half. Whiff% and SwStr% stabilize ",
      "at the same rate for hitters, and both are more stable than K% itself."),

    div(style = HW_CARD,
      HTML(hw_table_html(
        data.frame(
          Metric  = c("Whiff%", "SwStr%", "K%", "CSW%"),
          `Split-half r` = c("0.837", "0.841", "0.765", "0.704"),
          check.names = FALSE
        ),
        c("Metric", "Split-half r (100+ PA)"),
        highlight_row = 1
      )),
      p(style = "font-size: 0.82rem; color: #666; margin-top: 0.3rem; margin-bottom: 0;",
        "Whiff% and SwStr% are essentially tied for self-stability. But Whiff% is far more ",
        "predictive of K% — suggesting the extra information it carries about miss quality ",
        "matters more than the raw stabilization speed.")
    ),

    # ── Section 4: Conversion ────────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Converting to K%"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1.5rem;",
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
        HTML(hw_table_html(
          data.frame(
            Model = c("Whiff% → K%", "SwStr% → K%", "CSW% → K%"),
            Slope = c("0.873", "1.427", "1.637"),
            Intercept = c("+0.004", "+0.065", "-0.226"),
            `R²` = c("0.794", "0.589", "0.554"),
            Shortcut = c("K% ≈ Whiff% - 3pp", "K% ≈ 2 × SwStr% (weak)", "—"),
            check.names = FALSE
          ),
          c("Model", "Slope", "Intercept", "R²", "Shortcut"),
          highlight_row = 1
        )),
        p(style = "font-size: 0.82rem; color: #666; margin-top: 0.3rem; margin-bottom: 0;",
          "The SwStr% 2× shortcut works poorly for hitters: the slope is only 1.43 with a large ",
          "positive intercept (+6.5%), meaning it systematically overestimates K% for low-SwStr% hitters ",
          "and underestimates for high-SwStr% hitters. Stick with Whiff%.")
      ),
      div(style = paste0(HW_CARD, " flex: 1; min-width: 300px;"),
          plotOutput(ns("scatter_swstr_k"), height = "300px"))
    ),

    # ── Section 5: Key Takeaways ─────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Key Takeaways"),

    div(
      style = "font-size: 0.92rem; line-height: 1.7; color: #333; margin-bottom: 1.5rem;",

      div(style = paste0(HW_CARD, " border-left: 4px solid #4a6fa5;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #4a6fa5; margin-bottom: 0.3rem;",
                "Whiff% is even more dominant for hitters"),
        p("For pitchers, Whiff% edged out SwStr% in most comparisons. For hitters, it's not even close: ",
          "R²=0.794 vs 0.589. If you're evaluating a hitter's strikeout tendencies — whether for ",
          "early-season evaluation, trade targets, or projections — Whiff% is the clear first choice. ",
          "Subtract ~3 percentage points and you have their K%.")
      ),

      div(style = paste0(HW_CARD, " border-left: 4px solid #2f7d3a;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #2f7d3a; margin-bottom: 0.3rem;",
                "Hitter K-rates are stickier than pitcher K-rates"),
        p("Everything is more stable on the hitter side. YoY K% self-correlation is r=0.821 for hitters ",
          "vs 0.728 for pitchers. Whiff% YoY is 0.865 vs 0.741. This makes sense — a hitter's plate ",
          "discipline is largely self-determined, while pitchers face different lineups every start. ",
          "The practical implication: you can trust a hitter's K% profile to persist more confidently ",
          "than a pitcher's.")
      ),

      div(style = paste0(HW_CARD, " border-left: 4px solid #b77343;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #b77343; margin-bottom: 0.3rem;",
                "SwStr% shortcuts don't work well for hitters"),
        p("The pitcher article confirmed the K% ≈ 2.1 × SwStr% rule. For hitters, the ratio is 2.03× ",
          "but with R² of only 0.589 and a messy regression (slope=1.43, intercept=+6.5%). ",
          "The simple multiplier systematically misestimates across the K% spectrum. ",
          "There's no clean SwStr% shortcut for hitters — just use Whiff% directly.")
      )
    ),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: FanGraphs hitter leaderboards (SwStr%, CSW%, K%) and ",
      "Baseball Savant pitch-level data (Whiff%), 2015-2025 (excl. 2020). ",
      "200+ PA for full-season, 100+ PA per half for splits. ",
      "Whiff% = swinging strikes / total swings (Savant definition, excludes foul tips)."
    )
  )
}

# ── Module Server ────────────────────────────────────────────────────────────

hitterWhiffServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    raw <- load_hitter_whiff_raw()
    full <- if (!is.null(raw)) raw[raw$split == "full" & raw$pa >= 200, ] else NULL

    make_scatter <- function(d, x_col, y_col, x_lab, y_lab, colour) {
      r_val <- cor(d[[x_col]], d[[y_col]], use = "complete.obs")
      ggplot2::ggplot(d, ggplot2::aes(x = .data[[x_col]], y = .data[[y_col]])) +
        ggplot2::geom_point(alpha = 0.12, size = 1.5, colour = colour) +
        ggplot2::geom_smooth(method = "lm", se = FALSE, colour = colour, linewidth = 1.2) +
        ggplot2::annotate(
          "text",
          x = min(d[[x_col]], na.rm = TRUE) + diff(range(d[[x_col]], na.rm = TRUE)) * 0.03,
          y = max(d[[y_col]], na.rm = TRUE) - diff(range(d[[y_col]], na.rm = TRUE)) * 0.05,
          label = sprintf("r = %.3f  (R² = %.3f)", r_val, r_val^2),
          hjust = 0, size = 4.5, fontface = "bold", colour = "#333"
        ) +
        ggplot2::labs(x = x_lab, y = y_lab, title = paste(x_lab, "vs", y_lab)) +
        hitter_chart_theme()
    }

    output$scatter_whiff_k <- renderPlot({
      req(full)
      make_scatter(full, "whiff_pct", "k_pct", "Whiff%", "K%", "#4a6fa5")
    }, bg = "white")

    output$scatter_swstr_k <- renderPlot({
      req(full)
      make_scatter(full, "swstr_pct", "k_pct", "SwStr%", "K%", "#2f7d3a")
    }, bg = "white")
  })
}
