# ── mod_dl_compare.R ──────────────────────────────────────────────────────────
# Draft Lab Compare sub-module
# Three tabs: Hitters | Starting Pitchers | Hypothetical (hitters only)
# Data source: proj_results (aucValServer return) + adp_data from Draft Lab
# Table format: rows = players, cols = stats (Bootstrap striped)
# Inputs: 5 individual selectize dropdowns per player type

# ── Column definitions ────────────────────────────────────────────────────────

# Hitter display columns (roto vs points), always include ADP block
DLC_H_ROTO_COLS   <- c("name","team","positions","adp","adp_min_pick","adp_max_pick",
                        "pa","r","hr","rbi","sb","avg","obp","z_total_s","dollar_value")
DLC_H_ROTO_LABELS <- c("Name","Team","Pos","ADP","Min","Max",
                        "PA","R","HR","RBI","SB","AVG","OBP","Z-Score","$Value")

DLC_H_PTS_COLS    <- c("name","team","positions","adp","adp_min_pick","adp_max_pick",
                        "pa","r","hr","rbi","sb","avg","obp","total_pts","pts_per_g")
DLC_H_PTS_LABELS  <- c("Name","Team","Pos","ADP","Min","Max",
                        "PA","R","HR","RBI","SB","AVG","OBP","Total Pts","Pts/G")

DLC_P_ROTO_COLS   <- c("name","team","positions","adp","adp_min_pick","adp_max_pick",
                        "ip","w","k","sv","hd","era","whip","z_total_s")
DLC_P_ROTO_LABELS <- c("Name","Team","Pos","ADP","Min","Max",
                        "IP","W","K","SV","HD","ERA","WHIP","Z-Score")

DLC_P_PTS_COLS    <- c("name","team","positions","adp","adp_min_pick","adp_max_pick",
                        "ip","w","k","sv","hd","era","whip","total_pts","pts_per_ip")
DLC_P_PTS_LABELS  <- c("Name","Team","Pos","ADP","Min","Max",
                        "IP","W","K","SV","HD","ERA","WHIP","Total Pts","Pts/IP")

DLC_ROUNDING <- c(
  pa=1, r=1, hr=1, rbi=1, sb=1, avg=3, obp=3,
  ip=1, w=1, k=1, sv=1, hd=1, era=2, whip=2,
  z_total_s=2, dollar_value=0, adp=1, adp_min_pick=0, adp_max_pick=0,
  total_pts=1, pts_per_g=2, pts_per_ip=2
)

# ── Helpers ───────────────────────────────────────────────────────────────────

dlc_normalize_name <- function(x) tolower(gsub("[^a-zA-Z]", "", as.character(x)))

# Join ADP, min, max, positions from Draft Lab's adp_data
dlc_join_adp <- function(df, adp_data) {
  if (is.null(adp_data) || nrow(adp_data) == 0 || !("name" %in% names(df))) return(df)

  # Normalise join key
  df$dlc_jk <- dlc_normalize_name(df$name)

  nk_col <- if ("name_key" %in% names(adp_data)) "name_key" else {
    adp_data$dlc_jk2 <- dlc_normalize_name(
      if ("player_name" %in% names(adp_data)) adp_data$player_name else ""
    )
    "dlc_jk2"
  }

  want <- c(nk_col, "adp", "adp_min_pick", "adp_max_pick", "positions")
  want <- intersect(want, names(adp_data))
  adp_sub <- adp_data[, want, drop = FALSE]

  merged <- merge(df, adp_sub, by.x = "dlc_jk", by.y = nk_col, all.x = TRUE,
                  suffixes = c("", ".adp"))
  merged$dlc_jk  <- NULL
  if ("dlc_jk2" %in% names(merged)) merged$dlc_jk2 <- NULL

  # Resolve duplicate columns produced by merge suffixes
  for (col in c("adp", "adp_min_pick", "adp_max_pick", "positions")) {
    dup <- paste0(col, ".adp")
    if (dup %in% names(merged)) {
      merged[[col]] <- merged[[dup]]
      merged[[dup]] <- NULL
    }
  }
  merged
}

# Character columns — returned as-is, no numeric coercion
DLC_CHAR_COLS <- c("name", "team", "positions")

# Format a single value for display in a cell
dlc_fmt <- function(val, col) {
  if (is.null(val) || length(val) == 0 || is.na(val)) return("\u2014")
  if (col %in% DLC_CHAR_COLS) return(as.character(val))
  if (col == "dollar_value") return(paste0("$", formatC(suppressWarnings(as.numeric(val)), format = "f", digits = 0)))
  rd <- if (col %in% names(DLC_ROUNDING)) DLC_ROUNDING[[col]] else 1
  formatC(suppressWarnings(as.numeric(val)), format = "f", digits = rd)
}

# Build a Bootstrap striped table: rows = players, cols = stats
dlc_player_table <- function(df, display_cols, labels) {
  if (is.null(df) || nrow(df) == 0) return(NULL)

  # Keep only cols that exist; pair with their labels
  present  <- display_cols %in% names(df)
  use_cols <- display_cols[present]
  use_labs <- labels[present]

  # Format each cell
  rows <- lapply(seq_len(nrow(df)), function(i) {
    cells <- lapply(seq_along(use_cols), function(j) {
      col <- use_cols[j]
      val <- df[[col]][i]
      tags$td(dlc_fmt(val, col))
    })
    tags$tr(cells)
  })

  div(class = "table-responsive",
    tags$table(class = "table table-sm table-striped",
      tags$thead(tags$tr(lapply(use_labs, tags$th))),
      tags$tbody(rows)
    )
  )
}

# ── Hypothetical recalculation (per-PA-scenario) ──────────────────────────────

dlc_recalc_one_pa <- function(player_row, custom_pa, roto_params, scoring_mode,
                               pts_spec = NULL, min_bid = 1) {
  orig_pa <- suppressWarnings(as.numeric(player_row[["pa"]]))
  if (!is.finite(orig_pa) || orig_pa == 0) return(NULL)
  scale <- custom_pa / orig_pa

  new_row      <- player_row
  new_row$pa   <- custom_pa

  if (scoring_mode == "roto") {
    if (is.null(roto_params) || is.null(roto_params$params)) return(NULL)
    p         <- roto_params$params
    rate_cats <- p$is_rate
    for (cat in p$selected_cats) {
      if (!(cat %in% rate_cats) && cat %in% names(new_row))
        new_row[[cat]] <- player_row[[cat]] * scale
    }
    z_orig  <- suppressWarnings(as.numeric(player_row[["z_total_s"]])); if (is.na(z_orig)) z_orig <- 0
    sds     <- p$sds
    delta_z <- sum(vapply(p$selected_cats, function(cat) {
      if (cat %in% rate_cats) return(0)
      if (!(cat %in% names(sds)) || is.na(sds[[cat]]) || sds[[cat]] == 0) return(0)
      orig_val <- suppressWarnings(as.numeric(player_row[[cat]]))
      if (is.na(orig_val)) return(0)
      (orig_val * (scale - 1)) * (1 / (orig_pa * sds[[cat]]))
    }, numeric(1L)), na.rm = TRUE)
    new_z   <- z_orig + delta_z
    new_dv  <- max((new_z - roto_params$z_replacement) * roto_params$dollars_per_z + min_bid, min_bid)

    c(pa = custom_pa,
      r   = if ("r"   %in% names(new_row)) round(new_row$r,   1) else NA,
      hr  = if ("hr"  %in% names(new_row)) round(new_row$hr,  1) else NA,
      rbi = if ("rbi" %in% names(new_row)) round(new_row$rbi, 1) else NA,
      sb  = if ("sb"  %in% names(new_row)) round(new_row$sb,  1) else NA,
      avg = if ("avg" %in% names(new_row)) round(new_row$avg, 3) else NA,
      obp = if ("obp" %in% names(new_row)) round(new_row$obp, 3) else NA,
      z_total_s   = round(new_z,  2),
      dollar_value = round(new_dv, 0))

  } else {
    if (!is.null(pts_spec)) {
      for (stat in names(pts_spec))
        if (stat %in% names(new_row)) new_row[[stat]] <- player_row[[stat]] * scale
    }
    total <- 0
    if (!is.null(pts_spec)) {
      for (s in names(pts_spec)) {
        v <- if (s %in% names(new_row)) suppressWarnings(as.numeric(new_row[[s]])) else 0
        if (!is.na(v)) total <- total + v * pts_spec[[s]]
      }
    }
    g_val <- if ("g" %in% names(player_row)) suppressWarnings(as.numeric(player_row$g)) else NA_real_
    c(pa = custom_pa,
      r   = if ("r"   %in% names(new_row)) round(new_row$r,   1) else NA,
      hr  = if ("hr"  %in% names(new_row)) round(new_row$hr,  1) else NA,
      rbi = if ("rbi" %in% names(new_row)) round(new_row$rbi, 1) else NA,
      sb  = if ("sb"  %in% names(new_row)) round(new_row$sb,  1) else NA,
      avg = if ("avg" %in% names(new_row)) round(new_row$avg, 3) else NA,
      obp = if ("obp" %in% names(new_row)) round(new_row$obp, 3) else NA,
      total_pts = round(total, 1),
      pts_per_g = if (!is.na(g_val) && g_val > 0) round(total / g_val, 2) else NA)
  }
}

# Build the 5-row PA scenario matrix table
dlc_hypo_table <- function(player_row, pa_targets, weights,
                            roto_params, scoring_mode, pts_spec) {
  rows <- lapply(seq_along(pa_targets), function(i) {
    res <- dlc_recalc_one_pa(player_row, pa_targets[i], roto_params, scoring_mode, pts_spec)
    if (is.null(res)) return(NULL)
    c(weight = round(weights[i], 3), res)
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) return(NULL)

  mat <- do.call(rbind, lapply(rows, function(r) as.data.frame(t(r), stringsAsFactors = FALSE)))
  rownames(mat) <- NULL

  if (scoring_mode == "roto") {
    col_labels <- c("Wt","PA","R","HR","RBI","SB","AVG","OBP","Z-Score","$Value")
    col_keys   <- c("weight","pa","r","hr","rbi","sb","avg","obp","z_total_s","dollar_value")
  } else {
    col_labels <- c("Wt","PA","R","HR","RBI","SB","AVG","OBP","Total Pts","Pts/G")
    col_keys   <- c("weight","pa","r","hr","rbi","sb","avg","obp","total_pts","pts_per_g")
  }

  keep <- col_keys[col_keys %in% names(mat)]
  labs <- col_labels[col_keys %in% names(mat)]

  table_rows <- lapply(seq_len(nrow(mat)), function(i) {
    cells <- lapply(seq_along(keep), function(j) {
      col <- keep[j]
      val <- mat[[col]][i]
      tags$td(dlc_fmt(suppressWarnings(as.numeric(val)), col))
    })
    tags$tr(cells)
  })

  div(class = "table-responsive",
    tags$table(class = "table table-sm table-striped",
      tags$thead(tags$tr(lapply(labs, tags$th))),
      tags$tbody(table_rows)
    )
  )
}

# Build dropdown choices from a data frame
dlc_choices <- function(df) {
  blank <- setNames("", "")
  if (is.null(df) || nrow(df) == 0) return(blank)
  labels <- paste0(df$name, " (", df$team, ")")
  c(blank, setNames(df$name, labels))
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
          "Head there, configure your league settings, and click Calculate.")
      )
    })

    # ── Main UI ───────────────────────────────────────────────────────────────
    output$compare_content <- renderUI({
      if (!has_results()) return(NULL)
      navset_pill(
        id = ns("comp_tabs"),
        nav_panel("Hitters",          div(style = "margin-top:16px;", uiOutput(ns("h_panel")))),
        nav_panel("Starting Pitchers", div(style = "margin-top:16px;", uiOutput(ns("p_panel")))),
        nav_panel("Hypothetical",      div(style = "margin-top:16px;", uiOutput(ns("hypo_panel"))))
      )
    })

    # ── Hitter panel: 5 inputs (stacked) + table ─────────────────────────────
    output$h_panel <- renderUI({
      hd <- h_data()
      ch <- dlc_choices(hd)
      tagList(
        div(class = "pag-panel",
          lapply(seq_len(5), function(i) {
            selectizeInput(
              ns(paste0("sel_h_", i)),
              label   = paste("Hitter", i),
              choices = ch,
              selected = isolate(input[[paste0("sel_h_", i)]] %||% ""),
              options = list(placeholder = "Select\u2026", create = FALSE,
                             selectOnTab = TRUE, closeAfterSelect = TRUE)
            )
          })
        ),
        div(style = "margin-top:16px;", uiOutput(ns("comp_h_table")))
      )
    })

    output$comp_h_table <- renderUI({
      keys <- vapply(seq_len(5), function(i) {
        v <- input[[paste0("sel_h_", i)]] %||% ""
        trimws(as.character(v))
      }, character(1L))
      keys <- unique(keys[nzchar(keys)])
      if (length(keys) == 0)
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); padding:20px 0;", "Select at least one hitter above.")))
      hd <- h_data()
      if (is.null(hd)) return(NULL)
      df <- hd[hd$name %in% keys, , drop = FALSE]
      if (nrow(df) == 0) return(NULL)
      # Preserve selection order
      df <- df[match(keys, df$name), , drop = FALSE]
      df <- df[!is.na(df$name), , drop = FALSE]
      cols   <- if (mode() == "roto") DLC_H_ROTO_COLS   else DLC_H_PTS_COLS
      labels <- if (mode() == "roto") DLC_H_ROTO_LABELS else DLC_H_PTS_LABELS
      div(class = "pag-panel", dlc_player_table(df, cols, labels))
    })

    # ── Pitcher panel: 5 inputs (stacked) + table ────────────────────────────
    output$p_panel <- renderUI({
      pd <- p_data()
      ch <- dlc_choices(pd)
      tagList(
        div(class = "pag-panel",
          lapply(seq_len(5), function(i) {
            selectizeInput(
              ns(paste0("sel_p_", i)),
              label   = paste("Pitcher", i),
              choices = ch,
              selected = isolate(input[[paste0("sel_p_", i)]] %||% ""),
              options = list(placeholder = "Select\u2026", create = FALSE,
                             selectOnTab = TRUE, closeAfterSelect = TRUE)
            )
          })
        ),
        div(style = "margin-top:16px;", uiOutput(ns("comp_p_table")))
      )
    })

    output$comp_p_table <- renderUI({
      keys <- vapply(seq_len(5), function(i) {
        v <- input[[paste0("sel_p_", i)]] %||% ""
        trimws(as.character(v))
      }, character(1L))
      keys <- unique(keys[nzchar(keys)])
      if (length(keys) == 0)
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); padding:20px 0;", "Select at least one pitcher above.")))
      pd <- p_data()
      if (is.null(pd)) return(NULL)
      df <- pd[pd$name %in% keys, , drop = FALSE]
      if (nrow(df) == 0) return(NULL)
      df <- df[match(keys, df$name), , drop = FALSE]
      df <- df[!is.na(df$name), , drop = FALSE]
      cols   <- if (mode() == "roto") DLC_P_ROTO_COLS   else DLC_P_PTS_COLS
      labels <- if (mode() == "roto") DLC_P_ROTO_LABELS else DLC_P_PTS_LABELS
      div(class = "pag-panel", dlc_player_table(df, cols, labels))
    })

    # ── Hypothetical panel ────────────────────────────────────────────────────
    PA_DEFAULTS <- c(200, 300, 450, 500, 600)
    WT_DEFAULTS <- c(0.15, 0.25, 0.30, 0.20, 0.10)

    output$hypo_panel <- renderUI({
      hd <- h_data()
      ch <- dlc_choices(hd)

      pa_vals <- vapply(seq_len(5), function(i) {
        v <- input[[paste0("hyp_pa_", i)]] %||% PA_DEFAULTS[i]
        suppressWarnings(as.numeric(v))
      }, numeric(1L))
      wt_vals <- vapply(seq_len(5), function(i) {
        v <- input[[paste0("hyp_wt_", i)]] %||% WT_DEFAULTS[i]
        suppressWarnings(as.numeric(v))
      }, numeric(1L))

      tagList(
        div(class = "pag-panel",
          div(style = "margin-bottom:12px;",
            selectizeInput(
              ns("hypo_player"),
              label   = "Hitter",
              choices = ch,
              selected = isolate(input$hypo_player %||% ""),
              options = list(placeholder = "Select a hitter\u2026", create = FALSE,
                             selectOnTab = TRUE, closeAfterSelect = TRUE)
            )
          ),
          tags$p(class = "dl-howto-title", style = "margin-bottom:6px;",
                 "PA Thresholds & Weights (probability of each outcome)"),
          div(
            style = "display:grid; grid-template-columns:repeat(5,1fr); gap:12px;",
            lapply(seq_len(5), function(i) {
              div(
                numericInput(ns(paste0("hyp_pa_", i)), label = paste("PA", i),
                             value = pa_vals[i], min = 1, step = 10),
                numericInput(ns(paste0("hyp_wt_", i)), label = paste("Weight", i),
                             value = wt_vals[i], min = 0, max = 1, step = 0.01)
              )
            })
          )
        ),
        div(style = "margin-top:16px;", uiOutput(ns("hypo_table")))
      )
    })

    output$hypo_table <- renderUI({
      req(input$hypo_player, nzchar(input$hypo_player))
      hd <- h_data()
      if (is.null(hd)) return(NULL)
      rows <- hd[hd$name == input$hypo_player, , drop = FALSE]
      if (nrow(rows) == 0) return(NULL)
      player_row <- rows[1, , drop = FALSE]

      pa_targets <- vapply(seq_len(5), function(i) {
        suppressWarnings(as.numeric(input[[paste0("hyp_pa_", i)]] %||% PA_DEFAULTS[i]))
      }, numeric(1L))
      if (any(!is.finite(pa_targets) | pa_targets <= 0))
        return(div(class = "pf-empty", p("All PA thresholds must be positive numbers.")))

      wt_raw <- vapply(seq_len(5), function(i) {
        v <- suppressWarnings(as.numeric(input[[paste0("hyp_wt_", i)]] %||% WT_DEFAULTS[i]))
        if (!is.finite(v) || v < 0) 0 else v
      }, numeric(1L))
      if (sum(wt_raw) <= 0)
        return(div(class = "pf-empty", p("At least one threshold weight must be > 0.")))
      weights <- wt_raw / sum(wt_raw)

      rp <- tryCatch(proj_results$roto_params_h(), error = function(e) NULL)
      ps <- tryCatch(proj_results$pts_spec_h(),    error = function(e) NULL)

      tbl <- dlc_hypo_table(player_row, pa_targets, weights, rp, mode(), ps)
      if (is.null(tbl))
        return(div(class = "pf-empty", p("Could not compute hypothetical scenarios.")))

      div(class = "pag-panel",
        tags$p(tags$strong(player_row$name),
               tags$span(style = "color:var(--muted); font-size:0.82rem;",
                         paste0(" \u2014 projected PA: ", round(player_row$pa, 0)))),
        tbl
      )
    })
  })
}
