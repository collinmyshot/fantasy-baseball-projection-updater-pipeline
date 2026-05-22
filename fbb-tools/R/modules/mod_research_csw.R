# mod_research_csw.R
# Research article: Are All Strikes Equally Useful?
# A predictive comparison of SwStr%, CSW%, and Whiff%
# FanGraphs pitcher data, 2015-2025 (excl. 2020)

# ── Data loaders ─────────────────────────────────────────────────────────────

load_csw_raw <- function() {
  path <- file.path("data", "processed", "csw_research_raw.csv")
  if (!file.exists(path)) return(NULL)
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

# ── Chart theme ──────────────────────────────────────────────────────────────

csw_chart_theme <- function() {
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

CSW_CARD <- "margin-bottom: 1.5rem; background: #fff; border: 1px solid #c9d7c5; border-radius: 12px; padding: 16px 20px 18px;"

# ── Helper: styled correlation table ─────────────────────────────────────────

cor_table_html <- function(rows, col_headers, highlight_row = NULL, caption = NULL) {
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

cswResearchUI <- function(id) {
  ns <- NS(id)

  div(
    class = "csw-page",
    style = "max-width: 960px; margin: 0 auto; padding: 2rem 1.5rem 4rem;",

    h2(style = "font-size: 1.6rem; font-weight: 700; margin-bottom: 0.3rem;",
       "Predicting Pitcher Strikeouts"),
    p(style = "font-size: 1.05rem; color: #555; margin-bottom: 0.2rem;",
      "SwStr% vs CSW% vs Whiff%: Which Tells You More?"),
    p(style = "color: #666; font-size: 0.85rem; margin-bottom: 1.5rem;",
      "FanGraphs pitcher data, 2015-2025 (excl. 2020). Pitchers with 60+ IP."),

    # ── Writeup ──────────────────────────────────────────────────────────────
    div(
      style = "margin-bottom: 2.5rem; font-size: 0.92rem; line-height: 1.7; color: #333;",

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Background"),
      p(tags$b("Swinging Strike Rate (SwStr%)"), " measures the percentage of total pitches ",
        "that result in a swing and miss. It has long been a go-to metric for evaluating ",
        "a pitcher's swing-and-miss ability and projecting strikeout upside."),
      p(tags$b("Called Strike + Whiff Rate (CSW%)"), ", introduced by Nick Pollack in 2018, ",
        "adds called strikes to the equation: CSW% = (Called Strikes + Whiffs) / Total Pitches. ",
        "The idea is that pitchers who generate both swinging and looking strikes ",
        "demonstrate command in addition to stuff."),
      p(tags$b("Whiff Rate (Whiff%)"), " uses the same numerator as SwStr% (swinging strikes) ",
        "but divides by total swings instead of total pitches: Whiff% = Swinging Strikes / Swings. ",
        "It isolates the miss-generating skill by asking: given a batter committed to swinging, ",
        "how often does this pitcher make him miss?"),
      p("The original Pitcher List article (2019) found CSW% to be a stronger predictor of SIERA ",
        "than SwStr% alone. This analysis revisits and extends that claim with a larger dataset ",
        "and a third contender, adding within-season and year-over-year predictiveness, ",
        "stabilization rates, and K% conversion formulas."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Data & Methodology"),
      p("Pitcher-level stats from FanGraphs for 2015-2025 (excluding the shortened 2020 season). ",
        "Population: all pitchers with 60+ IP in a season (n = 2,735 pitcher-seasons), ",
        "tagged as SP, RP, or Both based on the share of IP in starts vs. relief. ",
        "Half-season splits use All-Star break dates with a 30+ IP minimum per half. ",
        "Year-over-year pairs require 60+ IP in both consecutive seasons. ",
        "Whiff% is sourced from Baseball Savant pitch-level data (whiffs / swings), ",
        "using the same All-Star break dates as FanGraphs for 1H/2H splits."),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "The Headline: Whiff% Is Basically K%"),
      p("The most striking finding is how closely Whiff% tracks K%. Consider:"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$b("Same scale:"), " League-average Whiff% is 25.1% vs league-average K% of 22.8%. ",
                "A pitcher's Whiff% will be about 2 percentage points above their K%. ",
                "A 30% Whiff% pitcher is roughly a 27.5% K% pitcher — no multiplication needed."),
        tags$li(tags$b("Stabilizes faster than K% itself:"), " At just 50 TBF, Whiff% split-half reliability is ",
                "r=0.733 vs K% at r=0.668. Whiff% is more stable than the thing it's predicting."),
        tags$li(tags$b("91% as predictive as K% itself:"), " Year-over-year, Whiff% predicts next-year K% at ",
                "r=0.665, while K% predicts itself at r=0.728. No other peripheral metric comes close."),
        tags$li(tags$b("Conversion is trivial:"), " K% ≈ Whiff% - 2pp. Slope of 0.971 with near-zero intercept. ",
                "Compare to SwStr%'s 2.1× multiplier or CSW%'s awkward negative-intercept formula.")
      ),
      p("In short: Whiff% is a ", tags$b("faster-stabilizing leading indicator of K%"), " that sits on ",
        "essentially the same scale. When you want to know if a pitcher's early-season strikeout rate is real, ",
        "Whiff% gives you the answer sooner than K% itself will. ",
        "(To see how these same metrics work from the hitter's perspective, see the ",
        tags$a(href = "#", onclick = "Shiny.setInputValue('nav_to_hitter_whiff', Math.random())",
               style = "color: #4a6fa5; text-decoration: underline;",
               "Hitter K% Prediction"), " article.)"),

      tags$h4(style = "font-size: 1rem; font-weight: 600; margin-bottom: 0.4rem;",
              "Other Findings"),
      tags$ul(
        style = "margin: 0.3rem 0 0.8rem 1.2rem;",
        tags$li(tags$b("CSW% explains current SIERA best"), " (same-season r=-0.754) because called strikes ",
                "capture command/location quality. But SIERA already predicts its own future better than CSW% does ",
                "(YoY: SIERA→SIERA r=0.598 vs CSW%→SIERA r=-0.533), ",
                "so this advantage is descriptive rather than predictive."),
        tags$li(tags$b("CSW% stabilizes slowest:"), " It doesn't reach r=0.70 until ~300 TBF, ",
                "vs ~50 TBF for both SwStr% and Whiff%. The called strike component introduces noise ",
                "that takes much longer to settle."),
        tags$li(tags$b("SwStr%'s 2.1× rule holds:"), " The actual mean K%/SwStr% ratio is 2.08×. ",
                "The regression confirms it (slope=1.85, intercept≈0). Still a useful shortcut if ",
                "you think in per-pitch terms.")
      )
    ),

    # ── Section 1: Same-Season Correlations ──────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Same-Season Correlations"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "How well does each metric explain K% and SIERA in the same season?"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1.5rem;",
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 300px;"),
        HTML(cor_table_html(
          data.frame(
            Predictor = c("Whiff%", "SwStr%", "CSW%", "Whiff%", "SwStr%", "CSW%"),
            Target    = c("K%", "K%", "K%", "SIERA", "SIERA", "SIERA"),
            r         = c("0.872", "0.838", "0.825", "-0.674", "-0.701", "-0.754"),
            `R²`  = c("0.761", "0.703", "0.680", "0.454", "0.491", "0.568"),
            check.names = FALSE
          ),
          c("Predictor", "Target", "r", "R²"),
          highlight_row = 1,
          caption = "All pitchers (n = 2,735)"
        ))
      ),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 380px;"),
        HTML(cor_table_html(
          data.frame(
            Role      = c("SP", "SP", "SP", "SP", "SP", "SP",
                          "", "RP", "RP", "RP", "RP", "RP", "RP"),
            Predictor = c("Whiff%", "SwStr%", "CSW%", "Whiff%", "SwStr%", "CSW%",
                          "", "Whiff%", "SwStr%", "CSW%", "Whiff%", "SwStr%", "CSW%"),
            Target    = c("K%", "K%", "K%", "SIERA", "SIERA", "SIERA",
                          "", "K%", "K%", "K%", "SIERA", "SIERA", "SIERA"),
            r         = c("0.878", "0.852", "0.822", "-0.673", "-0.699", "-0.768",
                          "", "0.800", "0.734", "0.741", "-0.421", "-0.461", "-0.627"),
            `R²`  = c("0.772", "0.726", "0.676", "0.453", "0.489", "0.589",
                          "", "0.639", "0.539", "0.550", "0.177", "0.213", "0.394"),
            check.names = FALSE
          ),
          c("Role", "Predictor", "Target", "r", "R²"),
          highlight_row = 1,
          caption = "By role"
        ))
      )
    ),

    # ── Section 2: Predictiveness ────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Predictiveness"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "How well do first-half and prior-year metrics predict future outcomes?"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1.5rem;",
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 320px;"),
        HTML(cor_table_html(
          data.frame(
            `1H Metric` = c("Whiff%", "SwStr%", "CSW%", "Whiff%", "SwStr%", "CSW%",
                            "", "Whiff%", "SwStr%", "CSW%"),
            `2H Target` = c("K%", "K%", "K%", "SIERA", "SIERA", "SIERA",
                            "", "Whiff%", "SwStr%", "CSW%"),
            r           = c("0.666", "0.643", "0.587", "-0.478", "-0.498", "-0.511",
                            "", "0.755", "0.760", "0.663"),
            `R²`    = c("0.444", "0.414", "0.344", "0.228", "0.248", "0.261",
                            "", "0.571", "0.578", "0.439"),
            check.names = FALSE
          ),
          c("1H Metric", "2H Target", "r", "R²"),
          highlight_row = 1,
          caption = "1H → 2H (30+ IP each half, n = 1,670)"
        ))
      ),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 320px;"),
        HTML(cor_table_html(
          data.frame(
            `Year N` = c("Whiff%", "SwStr%", "CSW%", "Whiff%", "SwStr%", "CSW%", "",
                          "Whiff%", "SwStr%", "CSW%", "", "K%", "SIERA"),
            `Year N+1` = c("K%", "K%", "K%", "SIERA", "SIERA", "SIERA", "",
                            "Whiff%", "SwStr%", "CSW%", "", "K%", "SIERA"),
            r           = c("0.665", "0.639", "0.584", "-0.523", "-0.534", "-0.533", "",
                            "0.741", "0.737", "0.667", "", "0.728", "0.598"),
            `R²`    = c("0.442", "0.408", "0.342", "0.273", "0.285", "0.284", "",
                            "0.549", "0.543", "0.445", "", "0.530", "0.357"),
            check.names = FALSE
          ),
          c("Year N", "Year N+1", "r", "R²"),
          highlight_row = 1,
          caption = "Year-over-year (60+ IP both years, n = 1,340)"
        ))
      )
    ),

    # ── Section 3: Stabilization ─────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Stabilization"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "How quickly does each metric become reliable? Split-half reliability ",
      "(1H vs 2H correlation) at varying minimum TBF thresholds per half."),

    div(style = CSW_CARD, plotOutput(ns("stab_chart"), height = "360px")),
    DTOutput(ns("stab_table"), width = "100%"),

    # ── Section 4: Conversion Rates ──────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Conversion Rates: Predicting K%"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "The classic rule of thumb is K% ≈ 2.1 × SwStr%. ",
      "Does it hold? And does CSW% convert to K% just as cleanly?"),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1rem;",
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_swstr_k"), height = "340px")),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_csw_k"), height = "340px")),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_whiff_k"), height = "340px"))
    ),

    div(style = CSW_CARD,
      HTML(cor_table_html(
        data.frame(
          Model = c("Whiff% → K%", "SwStr% → K%", "CSW% → K%"),
          Slope = c("0.971", "1.849", "1.763"),
          Intercept = c("-0.016", "+0.025", "-0.259"),
          `R²` = c("0.761", "0.703", "0.680"),
          `Shortcut` = c("K% ≈ Whiff%", "K% ≈ 2.08 × SwStr%", "—"),
          check.names = FALSE
        ),
        c("Model", "Slope", "Intercept", "R²", "Shortcut"),
        highlight_row = 1
      )),
      p(style = "font-size: 0.82rem; color: #666; margin-top: 0.5rem;",
        "Whiff% has the cleanest conversion: slope≈1 and intercept≈0 means K% is nearly equal to Whiff%. ",
        "The 2.1× rule for SwStr% is confirmed (actual mean = 2.08×). ",
        "CSW% has a large negative intercept (-0.259), making quick mental math harder.")
    ),

    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 0.5rem; font-weight: 600;",
      "By role:"),
    div(style = CSW_CARD,
      HTML(cor_table_html(
        data.frame(
          Role  = c("SP", "SP", "SP", "", "RP", "RP", "RP"),
          Model = c("Whiff% → K%", "SwStr% → K%", "CSW% → K%", "",
                    "Whiff% → K%", "SwStr% → K%", "CSW% → K%"),
          Slope = c("1.000", "1.919", "1.774", "", "0.891", "1.688", "1.591"),
          Intercept = c("-0.023", "+0.018", "-0.264", "", "+0.002", "+0.043", "-0.211"),
          `R²` = c("0.772", "0.726", "0.676", "", "0.639", "0.539", "0.550"),
          `K%/SwStr%` = c("—", "2.09×", "—", "", "—", "2.07×", "—"),
          check.names = FALSE
        ),
        c("Role", "Model", "Slope", "Intercept", "R²", "K%/SwStr%")
      ))
    ),

    # ── Section 5: Scatterplots vs SIERA ─────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Predicting SIERA"),
    p(style = "font-size: 0.88rem; color: #555; margin-bottom: 1rem;",
      "CSW%'s edge shows up here. The extra information from called strikes ",
      "tightens the scatter around the regression line, while Whiff% (lacking the ",
      "command component) is the weakest of the three."),

    div(
      style = "display: flex; flex-wrap: wrap; gap: 1.5rem; margin-bottom: 1rem;",
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_swstr_siera"), height = "340px")),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_csw_siera"), height = "340px")),
      div(style = paste0(CSW_CARD, " flex: 1; min-width: 280px;"),
          plotOutput(ns("scatter_whiff_siera"), height = "340px"))
    ),

    # ── Section 6: Key Takeaways ────────────────────────────────────────────
    tags$hr(style = "margin: 2rem 0; border-color: #ccc;"),
    h3(style = "font-size: 1.2rem; font-weight: 600; margin-bottom: 0.5rem;",
       "Key Takeaways: When to Use What"),

    div(
      style = "font-size: 0.92rem; line-height: 1.7; color: #333; margin-bottom: 1.5rem;",

      div(style = paste0(CSW_CARD, " border-left: 4px solid #4a6fa5;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #4a6fa5; margin-bottom: 0.3rem;",
                "Whiff% — Your early-season K% crystal ball"),
        p("It's April, a pitcher has 30-50 batters faced, and you want to know if their K% is for real. ",
          "Look at Whiff%. It stabilizes ", tags$b("faster than K% itself"), " (r=0.733 vs 0.668 at 50 TBF) ",
          "and converts trivially: subtract ~2 percentage points and you have their expected K%. ",
          "If a pitcher has a 28% Whiff% after 3 starts, you can be fairly confident their true-talent K% ",
          "is in the 25-26% range — well before K% itself has stabilized enough to trust."),
        p(style = "font-size: 0.85rem; color: #555; margin-bottom: 0;",
          tags$em("Best for: In-season evaluation, early reads, waiver wire pickups, streaming decisions."))
      ),

      div(style = paste0(CSW_CARD, " border-left: 4px solid #2f7d3a;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #2f7d3a; margin-bottom: 0.3rem;",
                "SwStr% — The familiar workhorse"),
        p("SwStr% stabilizes just as fast as Whiff% and is available everywhere ",
          "(FanGraphs, Savant, fantasy platforms). The 2.1× shortcut (SwStr% × 2 ≈ K%) ",
          "is slightly less precise than Whiff%'s conversion, but it's close. ",
          "SwStr% also captures a blend of miss skill AND the ability to generate swings — ",
          "which may matter for pitchers who get whiffs partly by inducing aggressive swings at bad pitches."),
        p(style = "font-size: 0.85rem; color: #555; margin-bottom: 0;",
          tags$em("Best for: Quick K% estimation, widely available, useful when Savant data isn't handy."))
      ),

      div(style = paste0(CSW_CARD, " border-left: 4px solid #b77343;"),
        tags$h4(style = "font-size: 0.95rem; font-weight: 700; color: #b77343; margin-bottom: 0.3rem;",
                "CSW% — Context, not prediction"),
        p("CSW% correlates best with SIERA in the same season (r=-0.754), but that's descriptive: ",
          "it explains current performance, not future performance. SIERA itself predicts its own future ",
          "better than CSW% does (YoY: r=0.598 vs r=-0.533). And CSW% takes ~300 TBF to stabilize — ",
          "six times longer than SwStr% or Whiff%. ",
          "CSW% can still tell you ", tags$em("why"), " a pitcher is succeeding (command + stuff vs. stuff alone), ",
          "but if your goal is forecasting K% or ERA, the other two metrics get you there faster and better."),
        p(style = "font-size: 0.85rem; color: #555; margin-bottom: 0;",
          tags$em("Best for: Understanding the nature of a pitcher's success (stuff vs. command), not forecasting."))
      )
    ),

    # ── Footer ───────────────────────────────────────────────────────────────
    div(
      style = "margin-top: 1.5rem; font-size: 0.78rem; color: #888;",
      "Source: FanGraphs pitcher leaderboards (SwStr%, CSW%, K%, SIERA) and ",
      "Baseball Savant pitch-level data (Whiff%), 2015-2025 (excl. 2020). ",
      "60+ IP for full-season analysis, 30+ IP per half for split-half, ",
      "60+ IP both years for year-over-year. ",
      "SwStr% = swinging strikes / total pitches. ",
      "CSW% = (called strikes + whiffs) / total pitches. ",
      "Whiff% = swinging strikes / total swings (Savant definition, excludes foul tips from contact)."
    )
  )
}

# ── Module Server ────────────────────────────────────────────────────────────

cswResearchServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    raw <- load_csw_raw()
    full <- if (!is.null(raw)) raw[raw$split == "full" & raw$ip >= 60, ] else NULL

    # ── Stabilization chart ──────────────────────────────────────────────────

    output$stab_chart <- renderPlot({
      stab_path <- file.path("data", "processed", "csw_stabilization.csv")
      req(file.exists(stab_path))
      stab <- utils::read.csv(stab_path, stringsAsFactors = FALSE)

      n_metrics <- if ("r_whiff" %in% names(stab)) 3 else 2
      if (n_metrics == 3) {
        stab_long <- data.frame(
          min_tbf = rep(stab$min_tbf, 3),
          metric  = rep(c("SwStr%", "CSW%", "Whiff%"), each = nrow(stab)),
          r       = c(stab$r_swstr, stab$r_csw, stab$r_whiff),
          stringsAsFactors = FALSE
        )
      } else {
        stab_long <- data.frame(
          min_tbf = rep(stab$min_tbf, 2),
          metric  = rep(c("SwStr%", "CSW%"), each = nrow(stab)),
          r       = c(stab$r_swstr, stab$r_csw),
          stringsAsFactors = FALSE
        )
      }

      ggplot2::ggplot(stab_long, ggplot2::aes(x = min_tbf, y = r,
                                                colour = metric, shape = metric)) +
        ggplot2::geom_line(linewidth = 1.2) +
        ggplot2::geom_point(size = 3) +
        ggplot2::geom_hline(yintercept = 0.70, linetype = "dashed", colour = "#999", linewidth = 0.5) +
        ggplot2::annotate("text", x = 360, y = 0.71, label = "r = 0.70 threshold",
                          size = 3.5, colour = "#999", hjust = 1) +
        ggplot2::scale_colour_manual(
          values = c("SwStr%" = "#2f7d3a", "CSW%" = "#b77343", "Whiff%" = "#4a6fa5"),
          name = NULL
        ) +
        ggplot2::scale_shape_manual(values = c("SwStr%" = 16, "CSW%" = 17, "Whiff%" = 15), name = NULL) +
        ggplot2::scale_x_continuous(breaks = seq(50, 400, by = 50)) +
        ggplot2::scale_y_continuous(limits = c(0.55, 0.85), breaks = seq(0.55, 0.85, by = 0.05)) +
        ggplot2::labs(
          x = "Minimum TBF per Half",
          y = "Split-Half Reliability (r)",
          title = "Stabilization: SwStr% vs CSW% vs Whiff%"
        ) +
        csw_chart_theme()
    }, bg = "white")

    output$stab_table <- renderDT({
      stab_path <- file.path("data", "processed", "csw_stabilization.csv")
      req(file.exists(stab_path))
      stab <- utils::read.csv(stab_path, stringsAsFactors = FALSE)
      if ("r_whiff" %in% names(stab)) {
        names(stab) <- c("Min TBF", "n", "r (SwStr%)", "r (CSW%)", "r (Whiff%)")
      } else {
        names(stab) <- c("Min TBF", "n", "r (SwStr%)", "r (CSW%)")
      }

      datatable(stab, rownames = FALSE, filter = "none", selection = "none",
                options = list(dom = "t", ordering = FALSE, pageLength = nrow(stab),
                               scrollX = FALSE,
                               columnDefs = list(list(className = "dt-center", targets = "_all"))),
                class = "display compact nowrap")
    })

    # ── Scatterplots ─────────────────────────────────────────────────────────

    make_csw_scatter <- function(d, x_col, y_col, x_lab, y_lab, colour) {
      r_val <- cor(d[[x_col]], d[[y_col]], use = "complete.obs")
      ggplot2::ggplot(d, ggplot2::aes(x = .data[[x_col]], y = .data[[y_col]])) +
        ggplot2::geom_point(alpha = 0.15, size = 1.5, colour = colour) +
        ggplot2::geom_smooth(method = "lm", se = FALSE, colour = colour, linewidth = 1.2) +
        ggplot2::annotate(
          "text",
          x = min(d[[x_col]], na.rm = TRUE) + diff(range(d[[x_col]], na.rm = TRUE)) * 0.03,
          y = max(d[[y_col]], na.rm = TRUE) - diff(range(d[[y_col]], na.rm = TRUE)) * 0.05,
          label = sprintf("r = %.3f  (R² = %.3f)", r_val, r_val^2),
          hjust = 0, size = 4.5, fontface = "bold", colour = "#333"
        ) +
        ggplot2::labs(x = x_lab, y = y_lab, title = paste(x_lab, "vs", y_lab)) +
        csw_chart_theme()
    }

    output$scatter_swstr_k <- renderPlot({
      req(full)
      make_csw_scatter(full, "swstr_pct", "k_pct", "SwStr%", "K%", "#2f7d3a")
    }, bg = "white")

    output$scatter_csw_k <- renderPlot({
      req(full)
      make_csw_scatter(full, "csw_pct", "k_pct", "CSW%", "K%", "#b77343")
    }, bg = "white")

    output$scatter_whiff_k <- renderPlot({
      req(full)
      make_csw_scatter(full, "whiff_pct", "k_pct", "Whiff%", "K%", "#4a6fa5")
    }, bg = "white")

    output$scatter_swstr_siera <- renderPlot({
      req(full)
      make_csw_scatter(full, "swstr_pct", "siera", "SwStr%", "SIERA", "#2f7d3a")
    }, bg = "white")

    output$scatter_csw_siera <- renderPlot({
      req(full)
      make_csw_scatter(full, "csw_pct", "siera", "CSW%", "SIERA", "#b77343")
    }, bg = "white")

    output$scatter_whiff_siera <- renderPlot({
      req(full)
      make_csw_scatter(full, "whiff_pct", "siera", "Whiff%", "SIERA", "#4a6fa5")
    }, bg = "white")
  })
}
