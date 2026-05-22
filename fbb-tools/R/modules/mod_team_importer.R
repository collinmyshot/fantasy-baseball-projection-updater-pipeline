# ── mod_team_importer.R ───────────────────────────────────────────────────────
# Team Importer: roster builder with 80th-percentile target comparison
# Depends on: proj_results list (result_h, result_p reactives from aucValServer),
#             adp_data reactive (shared Draft Lab ADP)

suppressPackageStartupMessages(library(jsonlite))

# ── Constants ─────────────────────────────────────────────────────────────────

TI_NFBC_PRESET <- list(
  C = 2L, `1B` = 1L, `2B` = 1L, SS = 1L, `3B` = 1L,
  OF = 5L, MI = 1L, CI = 1L, UT = 1L,
  P = 9L, Bench = 7L
)

TI_HIT_POS  <- c("C", "1B", "2B", "SS", "3B", "OF", "MI", "CI", "UT")
TI_PITCH_POS <- "P"
TI_BENCH_POS <- "Bench"
TI_ALL_POS   <- c(TI_HIT_POS, TI_PITCH_POS, TI_BENCH_POS)

# Position eligibility tags — what position tags qualify a player for each slot
TI_POS_ELIGIBLE <- list(
  C     = c("C"),
  `1B`  = c("1B"),
  `2B`  = c("2B"),
  SS    = c("SS"),
  `3B`  = c("3B"),
  OF    = c("OF"),
  MI    = c("2B", "SS"),
  CI    = c("1B", "3B"),
  UT    = c("C", "1B", "2B", "SS", "3B", "OF"),
  P     = c("P", "SP", "RP"),
  Bench = c("C", "1B", "2B", "SS", "3B", "OF", "P", "SP", "RP")
)

TI_MAX_SLOTS <- list(
  C = 3L, `1B` = 3L, `2B` = 3L, SS = 3L, `3B` = 3L,
  OF = 7L, MI = 2L, CI = 2L, UT = 2L,
  P = 14L, Bench = 10L
)

TI_H_TARGET_CATS <- c("AB", "R", "HR", "RBI", "SB", "AVG")
TI_P_TARGET_CATS <- c("W", "K", "SV", "ERA", "WHIP", "IP")
TI_LOW_BETTER    <- c("ERA", "WHIP")

TI_H_COL_MAP <- c(AB = "ab", R = "r", HR = "hr", RBI = "rbi", SB = "sb", AVG = "avg")
TI_P_COL_MAP <- c(W = "w", K = "k", SV = "sv", ERA = "era", WHIP = "whip", IP = "ip")

# Display labels and decimal places for proj stats tables
TI_COL_LABELS <- c(
  positions = "Pos", pa = "PA", ab = "AB", r = "R", hr = "HR", rbi = "RBI", sb = "SB",
  avg = "AVG", obp = "OBP",
  ip = "IP", w = "W", k = "K", sv = "SV", hd = "HD", svhd = "SVHD",
  era = "ERA", whip = "WHIP",
  dollar_value = "$Val", total_pts = "Pts",
  adp = "ADP", adp_range = "Min/Max", adp_min_pick = "Min", adp_max_pick = "Max",
  round = "Rd", overall_pick = "Pick"
)
TI_COL_ROUND <- c(
  pa = 0, ab = 0, r = 0, hr = 0, rbi = 0, sb = 0, avg = 3, obp = 3,
  ip = 1, w = 1, k = 0, sv = 1, hd = 1, svhd = 1, era = 2, whip = 2,
  dollar_value = 0, total_pts = 1,
  adp = 1, adp_range = 0, adp_min_pick = 0, adp_max_pick = 0,
  round = 0, overall_pick = 0
)

# Targets loaded once at source time
TI_TARGETS <- local({
  path <- "data/processed/2025_historical_targets_80th.csv"
  tryCatch({
    df <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
    names(df) <- trimws(names(df))
    df
  }, error = function(e) NULL)
})

# ── Helpers ───────────────────────────────────────────────────────────────────

# Compute overall draft pick from round, pick-in-round, league_teams
ti_overall_pick <- function(round, pick, teams) {
  if (is.na(round) || is.na(pick) || is.na(teams) ||
      round < 1 || pick < 1 || pick > teams) return(NA_integer_)
  if (round %% 2 == 1) {
    (round - 1L) * teams + pick
  } else {
    (round - 1L) * teams + (teams - pick + 1L)
  }
}

# Check whether a player's position string contains any eligible tag for a slot
ti_pos_eligible <- function(pos_str, eligible_tags) {
  if (is.na(pos_str) || !nzchar(pos_str)) return(FALSE)
  parts <- trimws(strsplit(pos_str, "[/,& ]+")[[1]])
  any(parts %in% eligible_tags)
}

# Input IDs for a slot
ti_slot_input_id <- function(pos, idx, suffix) {
  paste0("slot_", gsub("[^a-z0-9]", "", tolower(pos)), "_", idx, "_", suffix)
}

# Build a single slot row UI
# choices: named character vector for this position's selectize
# cur_sel: currently selected value (preserved across UI re-renders)
ti_slot_row_ui <- function(ns, pos, idx, choices = character(0), cur_sel = NULL) {
  player_id  <- ns(ti_slot_input_id(pos, idx, "player"))
  round_id   <- ns(ti_slot_input_id(pos, idx, "round"))
  overall_id <- ti_slot_input_id(pos, idx, "overall")  # output, not input

  div(
    class = "ti-slot-row",
    div(
      class = "ti-slot-pos",
      tags$span(class = "ti-pos-badge", pos)
    ),
    div(
      class = "ti-slot-player",
      selectizeInput(
        player_id, label = NULL,
        choices  = choices,
        selected = cur_sel,
        options  = list(
          placeholder    = paste("Search", pos, "players..."),
          maxItems       = 1L,
          closeAfterSelect = TRUE
        )
      )
    ),
    div(
      class = "ti-slot-round",
      numericInput(round_id, label = NULL, value = NA_integer_,
                   min = 1, max = 30, step = 1, width = "72px")
    ),
    div(
      class = "ti-slot-overall",
      textOutput(ns(overall_id), inline = TRUE)
    )
  )
}

# Build all slot rows for a given position
ti_position_slots_ui <- function(ns, pos, max_n) {
  lapply(seq_len(max_n), function(i) {
    div(
      id    = paste0("ti_slot_wrap_", gsub("[^a-z0-9]", "", tolower(pos)), "_", i),
      class = "ti-slot-wrap",
      ti_slot_row_ui(ns, pos, i)
    )
  })
}

# Build a target comparison table for one side (hitters or pitchers)
# show_per: render per-starter columns (Tgt/Str, Team/Str) — hitters only
# per_val: Tgt/Starter value; team_per_val: Team/Starter value
# Column order: Cat | Target (80th) | Team | Tgt/Str | Team/Str
ti_target_row <- function(cat, team_val, target_val, low_better = FALSE,
                           per_val = NA_real_, team_per_val = NA_real_,
                           show_per = FALSE) {
  fmt_val <- function(v, cat) {
    if (is.na(v)) return("-")
    if (cat %in% c("AVG", "ERA", "WHIP")) {
      formatC(v, digits = if (cat == "ERA") 2 else 3,
              format = "f", flag = "")
    } else {
      formatC(round(v, 1), digits = 1, format = "f")
    }
  }
  team_str   <- fmt_val(team_val, cat)
  target_str <- fmt_val(target_val, cat)

  # Compute color class: green if >5% better than target, red if >5% worse
  # No coloring if within ±5% window or data missing
  color_class <- ""
  if (!is.na(team_val) && !is.na(target_val) && target_val != 0) {
    ratio <- team_val / target_val
    if (!low_better) {
      if      (ratio >= 1.05) color_class <- "ti-tgt-good"
      else if (ratio <= 0.95) color_class <- "ti-tgt-bad"
    } else {
      # ERA, WHIP: lower is better
      if      (ratio <= 0.95) color_class <- "ti-tgt-good"
      else if (ratio >= 1.05) color_class <- "ti-tgt-bad"
    }
  }

  team_td_cls     <- paste0("ti-tgt-team",     if (nzchar(color_class)) paste0(" ", color_class) else "")
  team_per_td_cls <- paste0("ti-tgt-team-per", if (nzchar(color_class)) paste0(" ", color_class) else "")

  # Col order: Cat | Target (80th) | Team | [Tgt/Str | Team/Str]
  cells <- list(
    tags$td(class = "ti-tgt-cat", cat),
    tags$td(class = "ti-tgt-tgt", target_str),
    tags$td(class = team_td_cls,  team_str)
  )
  if (show_per) {
    cells <- c(cells, list(
      tags$td(class = "ti-tgt-per",     fmt_val(per_val,      cat)),
      tags$td(class = team_per_td_cls,  fmt_val(team_per_val, cat))
    ))
  }
  do.call(tags$tr, cells)
}

ti_target_table <- function(rows_html, title, show_per = FALSE) {
  div(
    class = "ti-tgt-block",
    div(class = "ti-tgt-title", title),
    tags$table(
      class = "ti-tgt-table",
      tags$thead(
        tags$tr(
          tags$th("Category"),
          tags$th("Target (80th)"),
          tags$th("Team"),
          if (show_per) tags$th(class = "ti-tgt-per-h", "Tgt/Str") else NULL,
          if (show_per) tags$th(class = "ti-tgt-per-h", "Team/Str") else NULL
        )
      ),
      tags$tbody(rows_html)
    )
  )
}

# ── UI ────────────────────────────────────────────────────────────────────────

teamImporterUI <- function(id) {
  ns <- NS(id)

  div(
    class = "ti-page",

    # ── Page header ──────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Draft Lab"),
      h1(class = "pf-title", "Team Importer"),
      p(class = "pf-subtitle",
        "Build your roster slot by slot, track projections for each pick, and
         compare your team totals against NFBC 80th-percentile winning targets.")
    ),

    # ── No-results banner (hidden when proj data is available) ───────────────
    uiOutput(ns("no_proj_banner")),

    # ── Main layout: controls left, content right ────────────────────────────
    uiOutput(ns("main_ui"))
  )
}

# ── Server ────────────────────────────────────────────────────────────────────

teamImporterServer <- function(id, proj_results, adp_data) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(wizard_step = 1L)

    # ── Target benchmark (auto-selected from league_teams) ───────────────────
    # Available targets and their team counts: ME = 15, OC = 12.
    # Pick whichever is closest; tie (13.5) breaks toward OC (12-team).
    TI_BENCH_TEAMS <- c("Main Event" = 15L, "Online Championship" = 12L)

    auto_draft_type <- reactive({
      n <- suppressWarnings(as.integer(input$league_teams %||% 12L))
      if (is.na(n) || n < 1) n <- 12L
      names(which.min(abs(TI_BENCH_TEAMS - n)))
    })

    output$benchmark_label <- renderUI({
      dt   <- auto_draft_type()
      tm   <- TI_BENCH_TEAMS[[dt]]
      tags$span(
        class = "ti-benchmark-val",
        dt, tags$span(class = "ti-benchmark-tm", paste0("(", tm, "-team)"))
      )
    })

    # ── Data availability ────────────────────────────────────────────────────
    has_proj <- reactive({
      tryCatch({
        h <- proj_results$result_h()
        p <- proj_results$result_p()
        !is.null(h) && is.data.frame(h) && nrow(h) > 0 &&
        !is.null(p) && is.data.frame(p) && nrow(p) > 0
      }, error = function(e) FALSE)
    })

    output$no_proj_banner <- renderUI({
      if (has_proj()) return(NULL)
      div(
        class = "ti-no-proj",
        div(class = "ti-no-proj-icon", HTML("&#x1F4CA;")),
        h4("Run Projections first"),
        p("Team Importer uses your Auction Value Calculator projections.
           Go to the Projections tab, load your data, and run the calculator."),
        actionButton(ns("go_proj"), "Go to Projections \u2192",
                     class = "btn btn-primary btn-sm")
      )
    })

    # ── Wizard step indicator ─────────────────────────────────────────────────
    output$ti_wizard_steps_ui <- renderUI({
      step  <- rv$wizard_step
      steps <- list(
        list(n = 1L, label = "League Setup"),
        list(n = 2L, label = "Build Roster"),
        list(n = 3L, label = "Review")
      )
      items <- list()
      for (i in seq_along(steps)) {
        s <- steps[[i]]
        circle_cls <- if (s$n == step) "wiz-circle active"
                      else if (s$n < step) "wiz-circle done"
                      else "wiz-circle"
        label_cls  <- if (s$n == step) "wiz-label active" else "wiz-label"
        content    <- if (s$n < step) HTML("&#x2713;") else as.character(s$n)
        items <- c(items, list(
          actionButton(ns(paste0("ti_goto_", s$n)),
            label = tagList(tags$span(class = circle_cls, content),
                            tags$span(class = label_cls, s$label)),
            class = "wiz-step-btn")
        ))
        if (i < length(steps)) {
          conn_cls <- if (s$n < step) "wiz-connector done" else "wiz-connector"
          items <- c(items, list(div(class = conn_cls)))
        }
      }
      div(class = "wiz-steps", tagList(items))
    })

    output$main_ui <- renderUI({
      if (!has_proj()) return(NULL)

      # Bake choices at render time so no separate updateSelectizeInput needed.
      cl <- pos_choices_list()
      cur_val <- function(pos, idx) {
        v <- isolate(input[[ti_slot_input_id(pos, idx, "player")]])
        if (is.null(v)) "" else v
      }

      tagList(
        # Hidden step tracker — initialised to current step so re-renders preserve position
        tags$div(style = "display:none;",
          numericInput(ns("ti_wizard_step"), NULL,
                       value = isolate(rv$wizard_step), min = 1L, max = 3L, step = 1L)
        ),

        uiOutput(ns("ti_wizard_steps_ui")),

        # ── Step 1 — League Setup ────────────────────────────────────────────
        conditionalPanel(
          condition = "input.ti_wizard_step == 1", ns = ns,
          div(
            class = "ti-wizard-step",
            layout_columns(
              col_widths = c(6, 6), gap = "20px",

              div(
                class = "pag-panel ti-setup-card",
                div(class = "ti-card-title", "Roster Setup"),
                div(
                  class = "ti-benchmark-label-wrap",
                  tags$span(class = "auc-field-label", "Target Benchmark"),
                  uiOutput(ns("benchmark_label"))
                ),
                div(
                  class = "ti-league-row",
                  div(class = "ti-league-field",
                    numericInput(ns("league_teams"), "League Teams",
                                 value = isolate(input$league_teams) %||% 12L,
                                 min = 1L, step = 1L, width = "100%")
                  ),
                  div(class = "ti-league-field",
                    numericInput(ns("draft_pos"), "My Draft Position",
                                 value = isolate(input$draft_pos) %||% 1L,
                                 min = 1L, max = 20L, step = 1L, width = "100%")
                  )
                ),
                div(class = "ti-preset-row",
                  actionButton(ns("apply_nfbc"), "Apply NFBC Preset",
                               class = "btn btn-outline-secondary btn-sm ti-preset-btn")
                ),
                tags$hr(class = "ti-divider"),
                div(class = "ti-card-title", "Roster Slots"),
                div(class = "ti-slot-counts",
                  lapply(TI_ALL_POS, function(pos) {
                    cnt_key <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
                    div(class = "ti-count-row",
                      tags$label(class = "ti-count-label", pos),
                      numericInput(ns(cnt_key), label = NULL,
                                   value = isolate(input[[cnt_key]]) %||% TI_NFBC_PRESET[[pos]],
                                   min = 0L, max = TI_MAX_SLOTS[[pos]], step = 1L, width = "68px")
                    )
                  })
                )
              ),

              div(
                class = "pag-panel ti-io-card",
                div(class = "ti-card-title", "Save / Load"),
                div(
                  class = "ti-io-buttons",
                  downloadButton(ns("export_json"), "Export Roster",
                                 class = "btn btn-outline-secondary btn-sm",
                                 title = "Saves as .json"),
                  div(class = "ti-import-wrap",
                    fileInput(ns("import_json"), label = NULL,
                              accept = ".json", placeholder = "Choose .json file...",
                              buttonLabel = "Import Roster", width = "100%")
                  )
                )
              )
            ),
            div(class = "wiz-nav",
              actionButton(ns("ti_next_1"), "Continue \u2192", class = "btn btn-primary")
            )
          )
        ),

        # ── Step 2 — Build Roster ────────────────────────────────────────────
        conditionalPanel(
          condition = "input.ti_wizard_step == 2", ns = ns,
          div(
            class = "ti-wizard-step",
            div(
              class = "pag-panel ti-roster-card",
              div(class = "ti-card-title", "My Roster"),
              div(
                class = "ti-slot-header",
                div(class = "ti-slot-pos",     "Pos"),
                div(class = "ti-slot-player",  "Player"),
                div(class = "ti-slot-round",   "Rd"),
                div(class = "ti-slot-overall", "Pick")
              ),
              div(class = "ti-pos-group",
                div(class = "ti-pos-group-label", "Hitters"),
                lapply(TI_HIT_POS, function(pos) {
                  lapply(seq_len(TI_MAX_SLOTS[[pos]]), function(i) {
                    div(id    = paste0("tiwr_", gsub("[^a-z0-9]", "", tolower(pos)), "_", i),
                        class = "ti-slot-wrap",
                        ti_slot_row_ui(ns, pos, i, choices = cl[[pos]], cur_sel = cur_val(pos, i)))
                  })
                })
              ),
              div(class = "ti-pos-group",
                div(class = "ti-pos-group-label", "Pitchers"),
                lapply(seq_len(TI_MAX_SLOTS[["P"]]), function(i) {
                  div(id = paste0("tiwr_p_", i), class = "ti-slot-wrap",
                    ti_slot_row_ui(ns, "P", i, choices = cl[["P"]], cur_sel = cur_val("P", i)))
                })
              ),
              div(class = "ti-pos-group",
                div(class = "ti-pos-group-label", "Bench"),
                lapply(seq_len(TI_MAX_SLOTS[["Bench"]]), function(i) {
                  div(id = paste0("tiwr_bench_", i), class = "ti-slot-wrap",
                    ti_slot_row_ui(ns, "Bench", i,
                                   choices = cl[["Bench"]], cur_sel = cur_val("Bench", i)))
                })
              )
            ),
            div(class = "wiz-nav",
              actionButton(ns("ti_back_2"), "\u2190 Back",     class = "btn btn-outline-secondary"),
              actionButton(ns("ti_next_2"), "Continue \u2192", class = "btn btn-primary")
            )
          )
        ),

        # ── Step 3 — Review ──────────────────────────────────────────────────
        conditionalPanel(
          condition = "input.ti_wizard_step == 3", ns = ns,
          div(
            class = "ti-wizard-step",
            uiOutput(ns("proj_tables")),
            uiOutput(ns("target_panel")),
            div(class = "wiz-nav",
              actionButton(ns("ti_back_3"), "\u2190 Back", class = "btn btn-outline-secondary")
            )
          )
        )
      )
    })

    # ── Wizard navigation ─────────────────────────────────────────────────────
    wiz_go <- function(n) {
      rv$wizard_step <- n
      updateNumericInput(session, "ti_wizard_step", value = n)
    }
    observeEvent(input$ti_next_1,  wiz_go(2L), ignoreInit = TRUE)
    observeEvent(input$ti_next_2,  wiz_go(3L), ignoreInit = TRUE)
    observeEvent(input$ti_back_2,  wiz_go(1L), ignoreInit = TRUE)
    observeEvent(input$ti_back_3,  wiz_go(2L), ignoreInit = TRUE)
    observeEvent(input$ti_goto_1,  wiz_go(1L), ignoreInit = TRUE)
    observeEvent(input$ti_goto_2,  wiz_go(2L), ignoreInit = TRUE)
    observeEvent(input$ti_goto_3,  wiz_go(3L), ignoreInit = TRUE)

    # ── Player choices ────────────────────────────────────────────────────────
    # Build position-filtered choices for every slot position.
    # Primary source: adp_data (has position info); fallback: proj data.

    pos_choices_list <- reactive({
      adp <- adp_data()
      h_df <- tryCatch(proj_results$result_h(), error = function(e) NULL)
      p_df <- tryCatch(proj_results$result_p(), error = function(e) NULL)

      adp_ok  <- !is.null(adp) && nrow(adp) > 0 && "positions" %in% names(adp)
      h_ok    <- !is.null(h_df) && nrow(h_df) > 0
      p_ok    <- !is.null(p_df) && nrow(p_df) > 0

      # Helper: named choice vector from a data frame using adp position filter
      make_adp_choices <- function(eligible_tags) {
        if (!adp_ok) return(NULL)
        mask <- vapply(adp$positions, ti_pos_eligible,
                       FUN.VALUE = logical(1),
                       eligible_tags = eligible_tags)
        df <- adp[mask, , drop = FALSE]
        if (nrow(df) == 0) return(NULL)
        df <- df[order(df$adp, na.last = TRUE), , drop = FALSE]
        setNames(df$player_name, paste0(df$player_name, " (", df$team, ")"))
      }

      # Fallback: all hitters sorted alpha
      h_fallback <- if (h_ok) {
        df <- h_df[order(h_df$name), , drop = FALSE]
        setNames(df$name, paste0(df$name, " (", df$team, ")"))
      } else NULL

      # Fallback: all pitchers (SP + RP) sorted alpha — use agg_p_all when available
      all_p_df <- tryCatch(proj_results$agg_p_all(), error = function(e) NULL) %||% p_df
      all_p_ok <- !is.null(all_p_df) && nrow(all_p_df) > 0
      p_fallback <- if (all_p_ok) {
        df <- all_p_df[order(all_p_df$name), , drop = FALSE]
        setNames(df$name, paste0(df$name, " (", df$team, ")"))
      } else NULL

      # Build per-position lists
      result <- list()
      for (pos in TI_ALL_POS) {
        eligible <- TI_POS_ELIGIBLE[[pos]]
        choices  <- make_adp_choices(eligible)

        # Fallback when ADP unavailable or returned nothing
        if (is.null(choices)) {
          if (pos == "P") {
            choices <- p_fallback
          } else if (pos == "Bench") {
            choices <- c(h_fallback, p_fallback)
          } else {
            choices <- h_fallback
          }
        }

        # Prepend empty option so selectize starts with placeholder (not first player)
        # setNames("", "") at runtime is valid; c("" = "") at parse-time is not
        empty_opt <- setNames("", "")
        result[[pos]] <- if (!is.null(choices)) c(empty_opt, choices) else empty_opt
      }

      # Supplement P and Bench choices with pitchers from agg_p_all not in ADP.
      # Uses normalized name keys (player_nk) to avoid double-adding accented variants.
      # This covers relievers (e.g. Edwin Díaz) who have projections but no ADP entry.
      if (all_p_ok) {
        existing_p_nk <- player_nk(unname(result[["P"]]))
        proj_p_nk     <- player_nk(all_p_df$name)
        miss_mask     <- !proj_p_nk %in% existing_p_nk
        if (any(miss_mask)) {
          miss_df <- all_p_df[miss_mask, , drop = FALSE]
          # De-duplicate by normalized key (keep first occurrence per nk)
          miss_df <- miss_df[!duplicated(player_nk(miss_df$name)), , drop = FALSE]
          miss_df <- miss_df[order(miss_df$name), , drop = FALSE]
          miss_p_choices <- setNames(miss_df$name,
                                     paste0(miss_df$name, " (", miss_df$team, ")"))
          result[["P"]] <- c(result[["P"]], miss_p_choices)
          # Also add to Bench (avoid duplicates by nk)
          existing_bench_nk <- player_nk(unname(result[["Bench"]]))
          new_bench_mask    <- !player_nk(unname(miss_p_choices)) %in% existing_bench_nk
          if (any(new_bench_mask)) {
            result[["Bench"]] <- c(result[["Bench"]], miss_p_choices[new_bench_mask])
          }
        }
      }

      result
    })

    # ── NFBC preset ──────────────────────────────────────────────────────────
    observeEvent(input$apply_nfbc, {
      updateNumericInput(session, "league_teams", value = 15L)
      updateNumericInput(session, "draft_pos",    value = 1L)
      for (pos in TI_ALL_POS) {
        cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        updateNumericInput(session, cnt_id, value = TI_NFBC_PRESET[[pos]])
      }
    })

    # ── Slot visibility (toggle CSS class via custom JS message) ─────────────
    observe({
      vis_map <- list()
      for (pos in TI_ALL_POS) {
        cnt_id  <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt     <- input[[cnt_id]]
        if (is.null(cnt) || is.na(cnt)) next
        cnt     <- as.integer(cnt)
        max_n   <- TI_MAX_SLOTS[[pos]]
        pos_key <- gsub("[^a-z0-9]", "", tolower(pos))
        for (i in seq_len(max_n)) {
          wrap_id <- paste0("tiwr_", pos_key, "_", i)
          vis_map[[wrap_id]] <- (i <= cnt)
        }
      }
      session$sendCustomMessage("ti_slot_visibility", vis_map)
    })

    # ── Overall pick outputs ─────────────────────────────────────────────────
    for (pos in TI_ALL_POS) {
      local({
        p <- pos
        for (idx in seq_len(TI_MAX_SLOTS[[p]])) {
          local({
            i          <- idx
            overall_id <- ti_slot_input_id(p, i, "overall")
            round_id   <- ti_slot_input_id(p, i, "round")
            output[[overall_id]] <- renderText({
              rnd   <- input[[round_id]]
              teams <- input$league_teams
              pick  <- input$draft_pos
              ov    <- ti_overall_pick(
                round = suppressWarnings(as.integer(rnd)),
                pick  = suppressWarnings(as.integer(pick)),
                teams = suppressWarnings(as.integer(teams))
              )
              if (is.na(ov)) "-" else as.character(ov)
            })
          })
        }
      })
    }

    # ── Collect selected players ──────────────────────────────────────────────
    # Starters: non-bench hitter/pitcher slots. Bench: Bench slots only.
    # Team totals and targets use starters only; proj tables show both sections.

    selected_starters_h <- reactive({
      req(has_proj())
      names_vec <- character(0)
      for (pos in TI_HIT_POS) {
        cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt    <- as.integer(input[[cnt_id]] %||% TI_NFBC_PRESET[[pos]])
        if (is.na(cnt) || cnt == 0) next
        for (i in seq_len(cnt)) {
          sid <- ti_slot_input_id(pos, i, "player")
          val <- input[[sid]]
          if (!is.null(val) && nzchar(val)) names_vec <- c(names_vec, val)
        }
      }
      unique(names_vec)
    })

    selected_starters_p <- reactive({
      req(has_proj())
      names_vec <- character(0)
      cnt_p <- as.integer(input$cnt_p %||% TI_NFBC_PRESET[["P"]])
      if (!is.na(cnt_p) && cnt_p > 0) {
        for (i in seq_len(cnt_p)) {
          sid <- ti_slot_input_id("P", i, "player")
          val <- input[[sid]]
          if (!is.null(val) && nzchar(val)) names_vec <- c(names_vec, val)
        }
      }
      unique(names_vec)
    })

    # Bench players — split into hitters/pitchers for display sub-sections.
    # Uses normalized name matching to classify by projection pool.
    selected_bench_h <- reactive({
      req(has_proj())
      h_nk <- player_nk(proj_results$result_h()$name)
      bench_cnt <- as.integer(input$cnt_bench %||% TI_NFBC_PRESET[["Bench"]])
      if (is.na(bench_cnt) || bench_cnt == 0) return(character(0))
      names_vec <- character(0)
      for (i in seq_len(bench_cnt)) {
        sid <- ti_slot_input_id("Bench", i, "player")
        val <- input[[sid]]
        if (!is.null(val) && nzchar(val) && player_nk(val) %in% h_nk)
          names_vec <- c(names_vec, val)
      }
      unique(names_vec)
    })

    selected_bench_p <- reactive({
      req(has_proj())
      all_p <- tryCatch(proj_results$agg_p_all(), error = function(e) NULL) %||%
               tryCatch(proj_results$result_p(),   error = function(e) NULL)
      if (is.null(all_p)) return(character(0))
      p_nk <- player_nk(all_p$name)
      bench_cnt <- as.integer(input$cnt_bench %||% TI_NFBC_PRESET[["Bench"]])
      if (is.na(bench_cnt) || bench_cnt == 0) return(character(0))
      names_vec <- character(0)
      for (i in seq_len(bench_cnt)) {
        sid <- ti_slot_input_id("Bench", i, "player")
        val <- input[[sid]]
        if (!is.null(val) && nzchar(val) && player_nk(val) %in% p_nk)
          names_vec <- c(names_vec, val)
      }
      unique(names_vec)
    })

    # Legacy combined reactives (used in slot-visibility / export — keep for compatibility)
    selected_hitters  <- reactive(unique(c(selected_starters_h(), selected_bench_h())))
    selected_pitchers <- reactive(unique(c(selected_starters_p(), selected_bench_p())))

    # ── Draft slot info (round + overall pick per selected player) ─────────────
    selected_draft_info <- reactive({
      result <- list()
      teams  <- suppressWarnings(as.integer(input$league_teams %||% 12))
      dpos   <- suppressWarnings(as.integer(input$draft_pos    %||% 1))
      for (pos in TI_ALL_POS) {
        cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt    <- suppressWarnings(as.integer(input[[cnt_id]] %||% TI_NFBC_PRESET[[pos]]))
        if (is.na(cnt) || cnt == 0) next
        for (i in seq_len(cnt)) {
          pid <- ti_slot_input_id(pos, i, "player")
          rid <- ti_slot_input_id(pos, i, "round")
          nm  <- input[[pid]]
          rnd <- suppressWarnings(as.integer(input[[rid]]))
          if (!is.null(nm) && nzchar(nm)) {
            ov <- ti_overall_pick(rnd, dpos, teams)
            result[[nm]] <- list(round = rnd, overall = ov)
          }
        }
      }
      result
    })

    # ── Helper: join ADP and draft info columns onto a player data frame ───────
    ti_join_adp_draft <- function(df, adp, draft_info) {
      if (is.null(df) || nrow(df) == 0) return(df)

      # Join ADP columns (including positions)
      if (!is.null(adp) && nrow(adp) > 0 && "player_name" %in% names(adp)) {
        adp_need <- "player_name"
        if ("adp"         %in% names(adp)) adp_need <- c(adp_need, "adp")
        if ("adp_min_pick" %in% names(adp)) adp_need <- c(adp_need, "adp_min_pick", "adp_max_pick")
        if ("positions"   %in% names(adp)) adp_need <- c(adp_need, "positions")
        adp_sub <- adp[, adp_need, drop = FALSE]
        names(adp_sub)[1] <- "name"
        adp_sub <- adp_sub[!duplicated(adp_sub$name), , drop = FALSE]
        df <- merge(df, adp_sub, by = "name", all.x = TRUE)
      } else {
        df$adp          <- NA_real_
        df$adp_min_pick <- NA_integer_
        df$adp_max_pick <- NA_integer_
        df$positions    <- NA_character_
      }

      # Ensure min/max columns exist (guard for ADP without those cols)
      if (!"adp_min_pick" %in% names(df)) df$adp_min_pick <- NA_integer_
      if (!"adp_max_pick" %in% names(df)) df$adp_max_pick <- NA_integer_
      if (!"positions"    %in% names(df)) df$positions    <- NA_character_

      # Create pre-formatted adp_range column (e.g. "84/108")
      df$adp_range <- ifelse(
        !is.na(df$adp_min_pick) & !is.na(df$adp_max_pick),
        paste0(as.integer(df$adp_min_pick), "/", as.integer(df$adp_max_pick)),
        "-"
      )

      # Join round + overall_pick from slot inputs
      df$round <- sapply(df$name, function(nm) {
        info <- draft_info[[nm]]
        if (!is.null(info)) as.integer(info$round) else NA_integer_
      })
      df$overall_pick <- sapply(df$name, function(nm) {
        info <- draft_info[[nm]]
        if (!is.null(info)) as.integer(info$overall) else NA_integer_
      })
      df
    }

    # Helper: filter a projection df by a vector of selected names using normalized keys.
    # This handles accent/diacritic mismatches (e.g. "Edwin Díaz" vs "Edwin Diaz").
    ti_filter_by_name <- function(src_df, sel_names) {
      if (is.null(src_df) || nrow(src_df) == 0 || length(sel_names) == 0) return(src_df[0, ])
      sel_nk <- player_nk(sel_names)
      src_nk <- player_nk(src_df$name)
      src_df[src_nk %in% sel_nk, , drop = FALSE]
    }

    team_h_df <- reactive({
      sel <- selected_starters_h()
      if (length(sel) == 0) return(NULL)
      h_df <- ti_filter_by_name(proj_results$result_h(), sel)
      ti_join_adp_draft(h_df, adp_data(), selected_draft_info())
    })

    bench_h_df <- reactive({
      sel <- selected_bench_h()
      if (length(sel) == 0) return(NULL)
      h_df <- ti_filter_by_name(proj_results$result_h(), sel)
      ti_join_adp_draft(h_df, adp_data(), selected_draft_info())
    })

    team_p_df <- reactive({
      sel <- selected_starters_p()
      if (length(sel) == 0) return(NULL)
      p_src <- tryCatch(proj_results$agg_p_all(), error = function(e) NULL) %||%
               proj_results$result_p()
      p_df <- ti_filter_by_name(p_src, sel)
      ti_join_adp_draft(p_df, adp_data(), selected_draft_info())
    })

    bench_p_df <- reactive({
      sel <- selected_bench_p()
      if (length(sel) == 0) return(NULL)
      p_src <- tryCatch(proj_results$agg_p_all(), error = function(e) NULL) %||%
               proj_results$result_p()
      p_df <- ti_filter_by_name(p_src, sel)
      ti_join_adp_draft(p_df, adp_data(), selected_draft_info())
    })

    # ── Projected stats tables ───────────────────────────────────────────────

    # Build data rows for one section of a projection table (no <table> wrapper).
    ti_proj_rows <- function(df, cols, rnd) {
      lapply(seq_len(nrow(df)), function(i) {
        row <- df[i, ]
        stat_cells <- lapply(seq_along(cols), function(j) {
          col     <- cols[j]
          raw_val <- if (col %in% names(row)) row[[col]] else NA
          val     <- suppressWarnings(as.numeric(raw_val))
          dec     <- rnd[[col]]
          txt <- if (!is.na(val)) {
            if (!is.null(dec)) formatC(round(val, dec), digits = dec, format = "f")
            else as.character(val)
          } else {
            chr <- if (!is.null(raw_val) && !is.na(raw_val)) as.character(raw_val) else ""
            if (nzchar(chr) && chr != "NA") chr else "-"
          }
          tags$td(class = "ti-pt-stat", txt)
        })
        tags$tr(tags$td(class = "ti-pt-name", row$name), stat_cells)
      })
    }

    # Build a single <table> covering starters + optional bench rows so column
    # widths are shared (no misalignment from separate tables).
    ti_proj_table <- function(df, cols, labels, rnd, bench_df = NULL) {
      n_cols <- 1L + length(cols)  # player name + stat cols
      if (is.null(df) || nrow(df) == 0) {
        return(p(class = "ti-empty", "No players selected yet."))
      }
      hdr_cells <- lapply(c("Player", labels), function(l) {
        tags$th(class = if (l == "Player") "ti-pt-name-h" else "ti-pt-stat-h", l)
      })
      starter_rows <- ti_proj_rows(df, cols, rnd)
      bench_rows <- if (!is.null(bench_df) && nrow(bench_df) > 0) {
        bench_sep <- tags$tr(
          class = "ti-bench-sep-row",
          tags$td(class = "ti-bench-sep-cell", colspan = n_cols, "Bench")
        )
        c(list(bench_sep), ti_proj_rows(bench_df, cols, rnd))
      } else NULL
      tags$table(
        class = "ti-proj-table",
        tags$thead(tags$tr(hdr_cells)),
        tags$tbody(c(starter_rows, bench_rows))
      )
    }

    output$proj_tables <- renderUI({
      req(has_proj())
      h_df   <- team_h_df()
      p_df   <- team_p_df()
      bh_df  <- bench_h_df()
      bp_df  <- bench_p_df()

      tryCatch({
        # Pull selected scoring categories from AUC calc (same source as Compare tab)
        sel_h  <- tryCatch(proj_results$selected_cats_h(), error = function(e) NULL)
        sel_p  <- tryCatch(proj_results$selected_cats_p(), error = function(e) NULL)
        mode_v <- tryCatch(proj_results$scoring_mode(),    error = function(e) "roto")

        val_col <- if (identical(mode_v, "points")) "total_pts" else "dollar_value"

        make_cols <- function(ordered_cols) {
          cols   <- unique(ordered_cols)
          labels <- unname(sapply(cols, function(x) TI_COL_LABELS[[x]] %||% toupper(x)))
          rnd    <- as.list(setNames(
                      sapply(cols, function(x) if (x %in% names(TI_COL_ROUND)) TI_COL_ROUND[[x]] else 1),
                      cols))
          list(cols = cols, labels = labels, rnd = rnd)
        }

        # Hitters: Position | ADP | Min/Max | PA | [stat cats] | $Val | Rd | Pick
        h_cats      <- if (!is.null(sel_h) && length(sel_h) > 0) sel_h else c("r", "hr", "rbi", "sb", "avg")
        h_stat_cats <- unique(c("pa", h_cats))
        hc <- make_cols(c("positions", "adp", "adp_range", h_stat_cats, val_col, "round", "overall_pick"))

        # Pitchers: IP + AUC-selected cats + ADP, Min, Max, Rd, Pick
        p_cats <- if (!is.null(sel_p) && length(sel_p) > 0) sel_p else c("w", "k", "sv", "era", "whip")
        pc <- make_cols(c(unique(c("ip", p_cats)), "adp", "adp_min_pick", "adp_max_pick", "round", "overall_pick"))

        # Build table with optional bench sub-section in a single <table>
        make_proj_panel <- function(title, starters_df, bench_df, spec) {
          div(
            class = "pag-panel ti-proj-card",
            div(class = "ti-card-title", title),
            div(class = "ti-proj-scroll",
              ti_proj_table(starters_df, spec$cols, spec$labels, spec$rnd,
                            bench_df = bench_df)
            )
          )
        }

        tagList(
          make_proj_panel("Projected Hitters",  h_df, bh_df, hc),
          make_proj_panel("Projected Pitchers", p_df, bp_df, pc)
        )
      }, error = function(e) {
        div(class = "pag-panel ti-proj-card",
            p(class = "text-danger",
              paste("Error rendering projection tables:", conditionMessage(e))))
      })
    })

    # ── Team totals + target comparison ─────────────────────────────────────

    team_totals <- reactive({
      h_df <- team_h_df()
      p_df <- team_p_df()

      h_totals <- if (!is.null(h_df) && nrow(h_df) > 0) {
        # Counting stats: sum; rate stats (AVG, OBP): weighted by PA/AB
        tot <- list()
        for (cat in names(TI_H_COL_MAP)) {
          # Special case: AB is computed as sum(H / AVG) since ab is not a direct column
          if (cat == "AB") {
            if ("h" %in% names(h_df) && "avg" %in% names(h_df)) {
              h_v  <- as.numeric(h_df$h)
              av_v <- as.numeric(h_df$avg)
              ok   <- !is.na(h_v) & !is.na(av_v) & av_v > 0
              tot[["AB"]] <- if (any(ok)) sum(h_v[ok] / av_v[ok]) else NA_real_
            } else {
              tot[["AB"]] <- NA_real_
            }
            next
          }
          col <- TI_H_COL_MAP[[cat]]
          if (!col %in% names(h_df)) { tot[[cat]] <- NA_real_; next }
          if (cat %in% c("AVG", "OBP")) {
            # Weighted average by PA
            pa_col  <- if ("pa" %in% names(h_df)) h_df$pa else rep(1, nrow(h_df))
            vals    <- h_df[[col]]
            valid   <- !is.na(vals) & !is.na(pa_col) & pa_col > 0
            if (!any(valid)) { tot[[cat]] <- NA_real_; next }
            tot[[cat]] <- sum(vals[valid] * pa_col[valid]) / sum(pa_col[valid])
          } else {
            tot[[cat]] <- sum(h_df[[col]], na.rm = TRUE)
          }
        }
        tot
      } else {
        setNames(rep(list(NA_real_), length(TI_H_TARGET_CATS)), TI_H_TARGET_CATS)
      }

      p_totals <- if (!is.null(p_df) && nrow(p_df) > 0) {
        tot <- list()
        for (cat in names(TI_P_COL_MAP)) {
          col <- TI_P_COL_MAP[[cat]]
          if (!col %in% names(p_df)) { tot[[cat]] <- NA_real_; next }
          if (cat %in% c("ERA", "WHIP")) {
            # Weighted by IP
            ip_col <- if ("ip" %in% names(p_df)) p_df$ip else rep(1, nrow(p_df))
            vals   <- p_df[[col]]
            valid  <- !is.na(vals) & !is.na(ip_col) & ip_col > 0
            if (!any(valid)) { tot[[cat]] <- NA_real_; next }
            tot[[cat]] <- sum(vals[valid] * ip_col[valid]) / sum(ip_col[valid])
          } else {
            tot[[cat]] <- sum(p_df[[col]], na.rm = TRUE)
          }
        }
        tot
      } else {
        setNames(rep(list(NA_real_), length(TI_P_TARGET_CATS)), TI_P_TARGET_CATS)
      }

      list(h = h_totals, p = p_totals)
    })

    output$target_panel <- renderUI({
      req(has_proj())
      totals <- team_totals()

      # Get target row
      draft_type <- auto_draft_type()
      tgt_row <- if (!is.null(TI_TARGETS)) {
        TI_TARGETS[TI_TARGETS[["Draft.Type"]] == draft_type, , drop = FALSE]
      } else NULL

      get_tgt <- function(cat) {
        if (is.null(tgt_row) || nrow(tgt_row) == 0) return(NA_real_)
        val <- tgt_row[[cat]]
        if (length(val) == 0 || is.na(val) || val == "") return(NA_real_)
        suppressWarnings(as.numeric(val))
      }

      # Count starting hitter slots (non-bench) for per-starter target
      n_h_starters <- sum(sapply(TI_HIT_POS, function(pos) {
        cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt <- suppressWarnings(as.integer(input[[cnt_id]] %||% TI_NFBC_PRESET[[pos]]))
        if (is.null(cnt) || is.na(cnt)) 0L else cnt
      }))

      # Counting stats get per-starter breakdown; rate stats (AVG) do not
      TI_H_COUNTING_CATS <- setdiff(TI_H_TARGET_CATS, c("AVG", "OBP"))

      # Hitter target rows — Team/Str and Tgt/Str columns for counting cats
      h_rows <- lapply(TI_H_TARGET_CATS, function(cat) {
        team_val   <- totals$h[[cat]]
        target_val <- get_tgt(cat)
        low_b      <- cat %in% TI_LOW_BETTER
        is_counting <- cat %in% TI_H_COUNTING_CATS
        team_per_val <- if (is_counting && !is.na(team_val) && n_h_starters > 0)
                          round(team_val / n_h_starters, 1) else NA_real_
        per_val      <- if (is_counting && !is.na(target_val) && n_h_starters > 0)
                          round(target_val / n_h_starters, 1) else NA_real_
        ti_target_row(cat, team_val, target_val, low_b,
                      per_val = per_val, team_per_val = team_per_val, show_per = TRUE)
      })

      # Pitcher target rows — no per-starter column
      p_rows <- lapply(TI_P_TARGET_CATS, function(cat) {
        team_val   <- totals$p[[cat]]
        target_val <- get_tgt(cat)
        low_b      <- cat %in% TI_LOW_BETTER
        ti_target_row(cat, team_val, target_val, low_b)
      })

      div(
        class = "pag-panel ti-tgt-card",
        div(class = "ti-card-title",
            paste("Team Totals vs.", draft_type, "80th Percentile Targets")),
        div(
          class = "ti-tgt-grid",
          ti_target_table(h_rows, "Hitting", show_per = TRUE),
          ti_target_table(p_rows, "Pitching")
        )
      )
    })

    # ── Export JSON ──────────────────────────────────────────────────────────

    export_state <- reactive({
      slots_list <- list()
      for (pos in TI_ALL_POS) {
        cnt_id  <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt     <- as.integer(input[[cnt_id]] %||% TI_NFBC_PRESET[[pos]])
        if (is.na(cnt) || cnt == 0) next
        for (i in seq_len(cnt)) {
          pid    <- ti_slot_input_id(pos, i, "player")
          rid    <- ti_slot_input_id(pos, i, "round")
          player <- input[[pid]] %||% ""
          round  <- input[[rid]] %||% NA_integer_
          slots_list <- c(slots_list, list(list(
            pos    = pos,
            idx    = i,
            player = player,
            round  = round
          )))
        }
      }

      cnt_map <- list()
      for (pos in TI_ALL_POS) {
        cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
        cnt_map[[pos]] <- as.integer(input[[cnt_id]] %||% TI_NFBC_PRESET[[pos]])
      }

      list(
        schema        = "team_importer_v1",
        league_teams  = as.integer(input$league_teams %||% 12L),
        draft_pos     = as.integer(input$draft_pos    %||% 1L),
        slot_counts   = cnt_map,
        slots         = slots_list
      )
    })

    output$export_json <- downloadHandler(
      filename = function() {
        paste0("team_importer_", format(Sys.Date(), "%Y%m%d"), ".json")
      },
      content = function(file) {
        jsonlite::write_json(export_state(), file, auto_unbox = TRUE, pretty = TRUE)
      }
    )

    # ── Import JSON ──────────────────────────────────────────────────────────

    observeEvent(input$import_json, {
      req(input$import_json)
      tryCatch({
        dat <- jsonlite::read_json(input$import_json$datapath, simplifyVector = FALSE)
        if (!identical(dat$schema, "team_importer_v1")) {
          showNotification("Unrecognized file format.", type = "error")
          return()
        }
        updateNumericInput(session, "league_teams",
                           value = dat$league_teams %||% 12L)
        updateNumericInput(session, "draft_pos",
                           value = dat$draft_pos %||% 1L)

        if (!is.null(dat$slot_counts)) {
          for (pos in names(dat$slot_counts)) {
            cnt_id <- paste0("cnt_", gsub("[^a-z0-9]", "", tolower(pos)))
            updateNumericInput(session, cnt_id, value = dat$slot_counts[[pos]])
          }
        }
        Sys.sleep(0.3)  # Allow slots to re-render before populating
        for (slot in dat$slots) {
          pid <- ti_slot_input_id(slot$pos, slot$idx, "player")
          rid <- ti_slot_input_id(slot$pos, slot$idx, "round")
          if (!is.null(slot$player) && nzchar(slot$player))
            updateSelectizeInput(session, pid, selected = slot$player)
          if (!is.null(slot$round) && !is.na(slot$round))
            updateNumericInput(session, rid, value = slot$round)
        }
        showNotification("Roster imported successfully.", type = "message")
      }, error = function(e) {
        showNotification(paste("Import failed:", conditionMessage(e)), type = "error")
      })
    })

    # ── Return value ─────────────────────────────────────────────────────────
    # Expose go_to_proj so parent (draftLabServer) can handle tab navigation
    list(
      go_to_proj = reactive(input$go_proj)
    )

  })
}
