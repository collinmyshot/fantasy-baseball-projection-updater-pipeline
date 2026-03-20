# ── mod_dl_compare.R ──────────────────────────────────────────────────────────
# Draft Lab Compare sub-module
# Three tabs: Hitters | Starting Pitchers | Hypothetical (hitters only)
# Data source: proj_results (aucValServer return) + adp_data from Draft Lab

# ── Constants ─────────────────────────────────────────────────────────────────

DLC_H_ROTO_COLS   <- c("pa","r","hr","rbi","sb","avg","obp","z_total_s","dollar_value","adp")
DLC_H_ROTO_LABELS <- c("PA","R","HR","RBI","SB","AVG","OBP","Z-Score","$Value","ADP")

DLC_H_PTS_COLS    <- c("pa","r","hr","rbi","sb","avg","obp","total_pts","pts_per_g","adp")
DLC_H_PTS_LABELS  <- c("PA","R","HR","RBI","SB","AVG","OBP","Total Pts","Pts/G","ADP")

DLC_P_ROTO_COLS   <- c("ip","w","k","sv","hd","era","whip","z_total_s","adp")
DLC_P_ROTO_LABELS <- c("IP","W","K","SV","HD","ERA","WHIP","Z-Score","ADP")

DLC_P_PTS_COLS    <- c("ip","w","k","sv","hd","era","whip","total_pts","pts_per_ip","adp")
DLC_P_PTS_LABELS  <- c("IP","W","K","SV","HD","ERA","WHIP","Total Pts","Pts/IP","ADP")

DLC_ROUNDING <- c(
  pa=0, r=0, hr=0, rbi=0, sb=0, avg=3, obp=3,
  ip=1, w=1, k=0, sv=1, hd=1, era=2, whip=2,
  z_total_s=2, dollar_value=0, adp=1,
  total_pts=1, pts_per_g=2, pts_per_ip=2
)

# Rows where a separator border-top is drawn above
DLC_SEP_ROWS <- c("z_total_s", "dollar_value", "total_pts", "adp")

# For best-value highlighting: lower is better for these
DLC_LOW_IS_BEST <- c("era", "whip", "adp")

# ── Helpers ───────────────────────────────────────────────────────────────────

dlc_normalize_name <- function(x) tolower(gsub("[^a-zA-Z]", "", as.character(x)))

# Attach ADP from Draft Lab's reactive adp_data
dlc_join_adp <- function(df, adp_data) {
  if (is.null(adp_data) || nrow(adp_data) == 0 || !("name" %in% names(df))) return(df)
  # Determine adp name key column (prefer name_key, fall back to normalizing player_name)
  if ("name_key" %in% names(adp_data)) {
    adp_sub <- adp_data[, c("name_key", "adp"), drop = FALSE]
    df$._nk  <- dlc_normalize_name(df$name)
    merged   <- merge(df, adp_sub, by.x = "._nk", by.y = "name_key", all.x = TRUE,
                      suffixes = c("", ".adp"))
    merged$`._nk` <- NULL
    # Prefer freshly-joined adp column; if there's already one, overwrite
    if ("adp.adp" %in% names(merged)) {
      merged$adp <- merged$adp.adp
      merged$adp.adp <- NULL
    }
    merged
  } else if ("player_name" %in% names(adp_data)) {
    adp_data$._nk <- dlc_normalize_name(adp_data$player_name)
    df$._nk        <- dlc_normalize_name(df$name)
    adp_sub        <- adp_data[, c("._nk", "adp"), drop = FALSE]
    merged         <- merge(df, adp_sub, by = "._nk", all.x = TRUE,
                            suffixes = c("", ".adp"))
    merged$`._nk` <- NULL
    if ("adp.adp" %in% names(merged)) {
      merged$adp <- merged$adp.adp
      merged$adp.adp <- NULL
    }
    merged
  } else {
    df
  }
}

# Format a single stat value for display
dlc_fmt <- function(val, col) {
  if (is.null(val) || length(val) == 0 || is.na(val)) return("\u2014")
  rd <- if (col %in% names(DLC_ROUNDING)) DLC_ROUNDING[[col]] else 1
  if (col == "dollar_value") return(paste0("$", formatC(val, format = "f", digits = 0)))
  formatC(val, format = "f", digits = rd)
}

# Build comparison table: rows = stats, cols = players
dlc_comp_table <- function(players_df, display_cols, labels) {
  if (is.null(players_df) || nrow(players_df) == 0) return(NULL)

  n_players    <- nrow(players_df)
  player_names <- players_df$name
  player_teams <- if ("team" %in% names(players_df)) players_df$team else rep("", n_players)

  header_cells <- tagList(
    tags$th(class = "pc-stat-head", "Stat"),
    lapply(seq_len(n_players), function(i) {
      tags$th(class = "pc-player-col",
        div(class = "pc-player-name", player_names[i]),
        div(class = "pc-player-team", player_teams[i])
      )
    })
  )

  stat_rows <- lapply(seq_along(display_cols), function(ri) {
    col   <- display_cols[ri]
    label <- labels[ri]

    vals <- vapply(seq_len(n_players), function(i) {
      v <- if (col %in% names(players_df)) players_df[[col]][i] else NA_real_
      suppressWarnings(as.numeric(v))
    }, numeric(1L))

    best_idx <- NA_integer_
    finite_v <- vals[is.finite(vals)]
    if (length(finite_v) > 1) {
      if (col %in% DLC_LOW_IS_BEST) {
        best_idx <- which(vals == min(vals, na.rm = TRUE))[1]
      } else {
        best_idx <- which(vals == max(vals, na.rm = TRUE))[1]
      }
    }

    row_class <- if (col %in% DLC_SEP_ROWS) "pc-sep-row" else ""
    tags$tr(class = row_class,
      tags$td(class = "pc-stat-label", label),
      lapply(seq_len(n_players), function(i) {
        cell_class <- paste("pc-stat-val",
                            if (!is.na(best_idx) && i == best_idx) "pc-best-val" else "")
        tags$td(class = trimws(cell_class), dlc_fmt(vals[i], col))
      })
    )
  })

  div(class = "pf-table-wrap pc-comp-wrap",
    tags$table(class = "pc-comp-table",
      tags$thead(tags$tr(header_cells)),
      tags$tbody(stat_rows)
    )
  )
}

# ── Hypothetical recalculation ─────────────────────────────────────────────────

dlc_recalc_hypo <- function(player_row, custom_pt, roto_params, is_hitter,
                             scoring_mode, pts_spec = NULL, min_bid = 1) {
  pt_col  <- if (is_hitter) "pa" else "ip"
  orig_pt <- suppressWarnings(as.numeric(player_row[[pt_col]]))
  if (is.null(orig_pt) || is.na(orig_pt) || orig_pt == 0) return(NULL)

  scale   <- custom_pt / orig_pt
  new_row <- player_row
  new_row[[pt_col]] <- custom_pt

  if (scoring_mode == "roto") {
    if (is.null(roto_params) || is.null(roto_params$params)) return(NULL)
    p         <- roto_params$params
    rate_cats <- p$is_rate
    for (cat in p$selected_cats) {
      if (!(cat %in% rate_cats) && cat %in% names(new_row)) {
        new_row[[cat]] <- player_row[[cat]] * scale
      }
    }
    sds    <- p$sds
    z_orig <- suppressWarnings(as.numeric(player_row[["z_total_s"]]))
    if (is.na(z_orig)) z_orig <- 0
    delta_z <- sum(vapply(p$selected_cats, function(cat) {
      if (cat %in% rate_cats) return(0)
      if (!(cat %in% names(sds)) || is.na(sds[[cat]]) || sds[[cat]] == 0) return(0)
      orig_val <- suppressWarnings(as.numeric(player_row[[cat]]))
      if (is.na(orig_val)) return(0)
      delta_stat <- orig_val * (scale - 1)
      ZperStat   <- 1 / (orig_pt * sds[[cat]])
      delta_stat * ZperStat
    }, numeric(1L)), na.rm = TRUE)

    new_z  <- z_orig + delta_z
    new_dv <- max(
      (new_z - roto_params$z_replacement) * roto_params$dollars_per_z + min_bid,
      min_bid
    )
    list(new_row = new_row, new_z = round(new_z, 2), new_dollar = round(new_dv, 0))

  } else {
    if (!is.null(pts_spec)) {
      for (stat in names(pts_spec)) {
        if (stat %in% names(new_row)) {
          new_row[[stat]] <- player_row[[stat]] * scale
        }
      }
    }
    total <- 0
    if (!is.null(pts_spec)) {
      for (s in names(pts_spec)) {
        v <- if (s %in% names(new_row)) suppressWarnings(as.numeric(new_row[[s]])) else 0
        if (!is.na(v)) total <- total + v * pts_spec[[s]]
      }
    }
    if (is_hitter) {
      g_val             <- if ("g" %in% names(new_row)) suppressWarnings(as.numeric(new_row$g)) else NA_real_
      new_row$total_pts <- round(total, 1)
      new_row$pts_per_g <- if (!is.na(g_val) && g_val > 0) round(total / g_val, 2) else NA_real_
    } else {
      new_row$total_pts  <- round(total, 1)
      new_row$pts_per_ip <- if (custom_pt > 0) round(total / custom_pt, 2) else NA_real_
    }
    list(new_row = new_row, new_pts = round(total, 1))
  }
}

# Build original | hypothetical side-by-side stat cards
dlc_hypo_cards <- function(orig_row, hypo_list, display_cols, labels, scoring_mode, is_hitter) {
  if (is.null(orig_row) || is.null(hypo_list)) return(NULL)
  hypo_row <- hypo_list$new_row

  make_stat_list <- function(row, is_hypo) {
    lapply(seq_along(display_cols), function(ri) {
      col   <- display_cols[ri]
      label <- labels[ri]
      val <- if (is_hypo) {
        if (scoring_mode == "roto") {
          if (col == "z_total_s")    hypo_list$new_z
          else if (col == "dollar_value") hypo_list$new_dollar
          else if (col %in% names(hypo_row)) hypo_row[[col]]
          else NA
        } else {
          if (col == "total_pts") hypo_list$new_pts
          else if (col %in% names(hypo_row)) hypo_row[[col]]
          else NA
        }
      } else {
        if (col %in% names(row)) row[[col]] else NA
      }
      sep     <- col %in% DLC_SEP_ROWS
      changed <- is_hypo && col != "adp" &&
                 !is.null(val) && !is.null(orig_row[[col]]) &&
                 !is.na(val)  && !is.na(orig_row[[col]]) &&
                 abs(suppressWarnings(as.numeric(val)) -
                     suppressWarnings(as.numeric(orig_row[[col]]))) > 0.001
      div(class = paste("pc-hypo-row", if (sep) "pc-hypo-sep" else ""),
        span(class = "pc-hypo-label", label),
        span(class = paste("pc-hypo-val", if (changed) "pc-hypo-changed" else ""),
             dlc_fmt(val, col))
      )
    })
  }

  layout_columns(
    col_widths = c(5, 7),
    div(class = "pag-panel pc-hypo-card",
      div(class = "pc-hypo-title", "Original Projection"),
      div(class = "pc-hypo-stat-list", make_stat_list(orig_row, FALSE))
    ),
    div(class = "pag-panel pc-hypo-card",
      div(class = "pc-hypo-title",
        "Hypothetical",
        span(class = "pc-hypo-pt-badge",
             paste0(if (is_hitter) "PA: " else "IP: ",
                    hypo_row[[if (is_hitter) "pa" else "ip"]]))
      ),
      div(class = "pc-hypo-stat-list", make_stat_list(hypo_row, TRUE))
    )
  )
}

# ── UI ────────────────────────────────────────────────────────────────────────

dlCompareUI <- function(id) {
  ns <- NS(id)
  div(
    class = "dl-compare-wrap",
    uiOutput(ns("no_data_banner")),
    uiOutput(ns("compare_content"))
  )
}

# ── Server ────────────────────────────────────────────────────────────────────

dlCompareServer <- function(id, proj_results, adp_data) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── Data accessors ────────────────────────────────────────────────────────
    h_data <- reactive({
      rh <- tryCatch(proj_results$result_h(), error = function(e) NULL)
      if (is.null(rh) || nrow(rh) == 0) return(NULL)
      dlc_join_adp(rh, adp_data())
    })

    p_data <- reactive({
      rp <- tryCatch(proj_results$result_p(), error = function(e) NULL)
      if (is.null(rp) || nrow(rp) == 0) return(NULL)
      dlc_join_adp(rp, adp_data())
    })

    has_results <- reactive(!is.null(h_data()) || !is.null(p_data()))

    mode <- reactive({
      m <- tryCatch(proj_results$scoring_mode(), error = function(e) "roto")
      if (is.null(m) || !m %in% c("roto", "points")) "roto" else m
    })

    # ── No-data banner ────────────────────────────────────────────────────────
    output$no_data_banner <- renderUI({
      if (has_results()) return(NULL)
      div(class = "pc-no-results",
        div(class = "pc-no-results-icon", "\U0001F4CA"),
        h4("Load Projections First"),
        p("Player Comparison uses projections calculated in the Projections tab.",
          "Head there, configure your league settings, and click Calculate."),
        actionButton(ns("go_proj"), "Go to Projections \u2192", class = "btn btn-primary")
      )
    })

    observeEvent(input$go_proj, {
      # Navigate to projections tab within Draft Lab
      nav_select("dl_tabs", "proj_tab", session = session)
    })

    # ── Main comparison UI ────────────────────────────────────────────────────
    output$compare_content <- renderUI({
      if (!has_results()) return(NULL)

      hd <- h_data()
      pd <- p_data()
      h_choices <- if (!is.null(hd) && nrow(hd) > 0)
        setNames(hd$name, paste0(hd$name, " (", hd$team, ")"))
      else character(0)
      p_choices <- if (!is.null(pd) && nrow(pd) > 0)
        setNames(pd$name, paste0(pd$name, " (", pd$team, ")"))
      else character(0)

      navset_pill(
        id = ns("comp_tabs"),

        # ── Hitters ────────────────────────────────────────────────────────────
        nav_panel(
          title = "Hitters",
          div(class = "pag-panel", style = "margin-top:16px;",
            div(class = "pc-selector-row",
              selectizeInput(
                ns("sel_h"),
                label   = "Select 2\u20135 hitters to compare:",
                choices = h_choices,
                multiple = TRUE,
                options  = list(
                  maxItems    = 5,
                  placeholder = "Search for a hitter\u2026",
                  plugins     = list("remove_button")
                )
              )
            ),
            uiOutput(ns("comp_h"))
          )
        ),

        # ── Starting Pitchers ──────────────────────────────────────────────────
        nav_panel(
          title = "Starting Pitchers",
          div(class = "pag-panel", style = "margin-top:16px;",
            div(class = "pc-selector-row",
              selectizeInput(
                ns("sel_p"),
                label    = "Select 2\u20135 starting pitchers to compare:",
                choices  = p_choices,
                multiple = TRUE,
                options  = list(
                  maxItems    = 5,
                  placeholder = "Search for a pitcher\u2026",
                  plugins     = list("remove_button")
                )
              )
            ),
            uiOutput(ns("comp_p"))
          )
        ),

        # ── Hypothetical ────────────────────────────────────────────────────────
        nav_panel(
          title = "Hypothetical",
          div(class = "pag-panel", style = "margin-top:16px;",
            div(class = "pc-hypo-controls",
              div(class = "pc-hypo-select-row",
                selectizeInput(
                  ns("hypo_player"),
                  label   = "Select a hitter:",
                  choices = h_choices,
                  options = list(placeholder = "Search\u2026")
                )
              ),
              uiOutput(ns("hypo_pt_ui"))
            ),
            div(style = "margin-top:20px;",
              uiOutput(ns("hypo_panel"))
            )
          )
        )
      )
    })

    # ── Hitter comparison table ───────────────────────────────────────────────
    output$comp_h <- renderUI({
      sel <- input$sel_h
      if (is.null(sel) || length(sel) < 2) {
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); padding:20px 0;",
            "Select at least 2 hitters above to compare.")))
      }
      hd <- h_data()
      if (is.null(hd)) return(NULL)
      df <- hd[hd$name %in% sel, , drop = FALSE]
      df <- df[match(sel, df$name), , drop = FALSE]
      df <- df[!is.na(df$name), , drop = FALSE]
      if (nrow(df) == 0) return(NULL)
      cols   <- if (mode() == "roto") DLC_H_ROTO_COLS   else DLC_H_PTS_COLS
      labels <- if (mode() == "roto") DLC_H_ROTO_LABELS else DLC_H_PTS_LABELS
      dlc_comp_table(df, cols, labels)
    })

    # ── Pitcher comparison table ──────────────────────────────────────────────
    output$comp_p <- renderUI({
      sel <- input$sel_p
      if (is.null(sel) || length(sel) < 2) {
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); padding:20px 0;",
            "Select at least 2 starting pitchers above to compare.")))
      }
      pd <- p_data()
      if (is.null(pd)) return(NULL)
      df <- pd[pd$name %in% sel, , drop = FALSE]
      df <- df[match(sel, df$name), , drop = FALSE]
      df <- df[!is.na(df$name), , drop = FALSE]
      if (nrow(df) == 0) return(NULL)
      cols   <- if (mode() == "roto") DLC_P_ROTO_COLS   else DLC_P_PTS_COLS
      labels <- if (mode() == "roto") DLC_P_ROTO_LABELS else DLC_P_PTS_LABELS
      dlc_comp_table(df, cols, labels)
    })

    # ── Hypothetical: player row ──────────────────────────────────────────────
    hypo_row <- reactive({
      req(input$hypo_player)
      hd <- h_data()
      if (is.null(hd)) return(NULL)
      rows <- hd[hd$name == input$hypo_player, , drop = FALSE]
      if (nrow(rows) == 0) return(NULL)
      rows[1, , drop = FALSE]
    })

    # ── Hypothetical: PA slider ───────────────────────────────────────────────
    output$hypo_pt_ui <- renderUI({
      req(hypo_row())
      pt_val <- suppressWarnings(as.numeric(hypo_row()[["pa"]]))
      if (is.na(pt_val)) pt_val <- 200
      sliderInput(ns("hypo_pt"), label = "Custom PA",
                  min = 1, max = 750, value = round(pt_val), step = 5)
    })

    # ── Hypothetical: recalculate ─────────────────────────────────────────────
    hypo_result <- reactive({
      req(hypo_row(), input$hypo_pt)
      rp <- tryCatch(proj_results$roto_params_h(), error = function(e) NULL)
      ps <- tryCatch(proj_results$pts_spec_h(),    error = function(e) NULL)
      dlc_recalc_hypo(
        player_row   = hypo_row(),
        custom_pt    = input$hypo_pt,
        roto_params  = rp,
        is_hitter    = TRUE,
        scoring_mode = mode(),
        pts_spec     = ps
      )
    })

    # ── Hypothetical: render ──────────────────────────────────────────────────
    output$hypo_panel <- renderUI({
      hr <- hypo_row()
      hy <- hypo_result()
      if (is.null(hr)) {
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); padding:20px 0;",
            "Select a hitter above to see the hypothetical projection.")))
      }
      cols   <- if (mode() == "roto") DLC_H_ROTO_COLS   else DLC_H_PTS_COLS
      labels <- if (mode() == "roto") DLC_H_ROTO_LABELS else DLC_H_PTS_LABELS
      dlc_hypo_cards(hr, hy, cols, labels, mode(), TRUE)
    })
  })
}
