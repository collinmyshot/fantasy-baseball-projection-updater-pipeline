# ── mod_dl_compare.R ──────────────────────────────────────────────────────────
# Draft Lab Compare sub-module
# Three tabs: Hitters | Starting Pitchers | Hypothetical (hitters only)
# Data source: proj_results (aucValServer return) + adp_data from Draft Lab
# Table format: rows = players, cols = stats (Bootstrap striped)
# Inputs: 5 individual selectize dropdowns per player type

# Maximum number of player slots in the Hitters / Pitchers compare panels
DLC_MAX_COMPARE_SLOTS <- 10L

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
                        "ip","w","k","sv","hd","era","whip")
DLC_P_ROTO_LABELS <- c("Name","Team","Pos","ADP","Min","Max",
                        "IP","W","K","SV","HD","ERA","WHIP")

DLC_P_PTS_COLS    <- c("name","team","positions","adp","adp_min_pick","adp_max_pick",
                        "ip","w","k","sv","hd","era","whip","total_pts","pts_per_ip")
DLC_P_PTS_LABELS  <- c("Name","Team","Pos","ADP","Min","Max",
                        "IP","W","K","SV","HD","ERA","WHIP","Total Pts","Pts/IP")

DLC_ROUNDING <- c(
  pa=0, r=1, hr=1, rbi=1, sb=1, avg=4, obp=4,
  ip=1, w=1, k=1, sv=1, hd=1, svhd=1, era=4, whip=3,
  z_total_s=2, dollar_value=2, adp=1, adp_min_pick=0, adp_max_pick=0,
  total_pts=1, pts_per_g=2, pts_per_ip=2
)

# Label lookup for any stat column
DLC_CAT_LABS <- c(
  name="Name", team="Team", positions="Pos",
  adp="ADP", adp_min_pick="Min", adp_max_pick="Max",
  # Hitter stats (roto + points)
  pa="PA", ab="AB", h="H", x1b="1B", x2b="2B", x3b="3B",
  r="R", hr="HR", rbi="RBI", sb="SB", cs="CS", bb="BB", hbp="HBP",
  avg="AVG", obp="OBP",
  # Pitcher stats (roto + points)
  ip="IP", w="W", k="K", sv="SV", hd="HD", svhd="SVHD", era="ERA", whip="WHIP",
  h_allow="H all.", bb_allow="BB iss.", hbp_bat="HBP bat.",
  hr_allow="HR all.", er="ER", qs="QS", bs="BS",
  # Score / value cols
  z_total_s="Z-Score", dollar_value="$Value",
  total_pts="Total Pts", pts_per_g="Pts/G", pts_per_ip="Pts/IP"
)

DLC_ID_BLOCK <- c("name","team","positions","adp","adp_min_pick","adp_max_pick")

# Build (cols, labels) list for display given selected scoring cats and mode
dlc_col_spec <- function(selected_cats, is_hitter, scoring_mode) {
  cats <- if (!is.null(selected_cats) && length(selected_cats) > 0)
    selected_cats
  else if (is_hitter) c("pa","r","hr","rbi","sb","avg")
  else                c("ip","w","k","sv","era","whip")

  score_cols <- if (scoring_mode == "roto") {
    if (is_hitter) c("z_total_s","dollar_value") else c()
  } else {
    if (is_hitter) c("total_pts","pts_per_g") else c("total_pts","pts_per_ip")
  }

  all_cols <- c(DLC_ID_BLOCK, cats, score_cols)
  # Use single-bracket [ not [[ so missing keys return NA rather than error
  labs     <- unname(sapply(all_cols, function(x) {
    v <- unname(DLC_CAT_LABS[x])
    if (!is.na(v)) v else x
  }))
  list(cols = all_cols, labels = labs)
}

# ── Helpers ───────────────────────────────────────────────────────────────────

# Delegates to shared player_nk() (utils_names.R)
dlc_normalize_name <- player_nk

# Join ADP, min, max, positions from Draft Lab's adp_data
dlc_join_adp <- function(df, adp_data) {
  if (is.null(adp_data) || nrow(adp_data) == 0 || !("name" %in% names(df))) return(df)

  # Normalise join key: name + team composite to avoid same-name players (e.g. Max Muncy LAD vs ATH)
  team_col_df <- if ("team" %in% names(df)) df$team else ""
  df$dlc_jk <- paste(dlc_normalize_name(df$name), dlc_normalize_name(team_col_df))

  team_col_adp <- if ("team" %in% names(adp_data)) adp_data$team else ""
  name_col_adp <- if ("player_name" %in% names(adp_data)) adp_data$player_name else ""
  adp_data$dlc_jk2 <- paste(dlc_normalize_name(name_col_adp), dlc_normalize_name(team_col_adp))
  nk_col <- "dlc_jk2"

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
  if (col == "dollar_value") return(paste0("$", formatC(suppressWarnings(as.numeric(val)), format = "f", digits = 2)))
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
    rate_cats      <- p$is_rate
    # vol_cats (avg, obp) are stored as is_rate for display purposes but their
    # z-scores scale with PA — H and H+BB are the true underlying counting stats.
    vol_cats       <- p$vol_cats %||% character(0)
    pure_rate_cats <- setdiff(rate_cats, vol_cats)

    # Scale counting stats proportionally with PA (vol_cats kept as rates for display)
    for (cat in p$selected_cats) {
      if (!(cat %in% rate_cats) && cat %in% names(new_row))
        new_row[[cat]] <- player_row[[cat]] * scale
    }

    # z-score decomposition:
    #   pure rate cats: z unchanged (nothing after AVG/OBP move to vol_cats)
    #   vol rate cats (AVG, OBP): z scales with PA — H and H+BB grow with opportunity
    #   count cats (HR, R, RBI, SB): z scales linearly with PA
    # At custom_pa == orig_pa, new_z == z_total_s exactly (matches main table)
    z_read <- function(cat) {
      v <- suppressWarnings(as.numeric(player_row[[paste0("z_", cat, "_s")]]))
      if (!is.na(v)) v else 0
    }
    cat_wt <- function(cat) {
      w <- roto_params$cat_weights[[cat]]
      if (is.null(w) || is.na(w)) 1.0 else max(0, w)
    }
    z_rate  <- sum(vapply(intersect(p$selected_cats, pure_rate_cats),
                          function(cat) cat_wt(cat) * z_read(cat), numeric(1L)), na.rm = TRUE)
    z_count <- sum(vapply(setdiff(p$selected_cats, pure_rate_cats),
                          function(cat) cat_wt(cat) * z_read(cat), numeric(1L)), na.rm = TRUE)
    new_z <- z_rate + z_count * scale
    # Additive PA volume z-score at the hypothetical PA
    pa_wt <- p$pa_weight %||% 0
    if (is.numeric(pa_wt) && is.finite(pa_wt) && pa_wt > 0) {
      pa_mu_h <- p$pa_mean
      pa_sd_h <- p$pa_sd
      if (is.finite(pa_mu_h) && is.finite(pa_sd_h) && pa_sd_h > 0)
        new_z <- new_z + ((custom_pa - pa_mu_h) / pa_sd_h) * pa_wt
    }
    new_dv  <- (new_z - roto_params$z_replacement) * roto_params$dollars_per_z + min_bid

    c(pa = custom_pa,
      r   = if ("r"   %in% names(new_row)) round(new_row$r,   1) else NA,
      hr  = if ("hr"  %in% names(new_row)) round(new_row$hr,  1) else NA,
      rbi = if ("rbi" %in% names(new_row)) round(new_row$rbi, 1) else NA,
      sb  = if ("sb"  %in% names(new_row)) round(new_row$sb,  1) else NA,
      avg = if ("avg" %in% names(new_row)) round(new_row$avg, 4) else NA,
      obp = if ("obp" %in% names(new_row)) round(new_row$obp, 4) else NA,
      z_total_s    = round(new_z,  2),
      dollar_value = round(new_dv, 2))

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
      avg = if ("avg" %in% names(new_row)) round(new_row$avg, 4) else NA,
      obp = if ("obp" %in% names(new_row)) round(new_row$obp, 4) else NA,
      total_pts = round(total, 1),
      pts_per_g = if (!is.na(g_val) && g_val > 0) round(total / g_val, 2) else NA)
  }
}

# Build the PA scenario matrix table, always including the player's projected PA (bolded)
dlc_hypo_table <- function(player_row, pa_targets,
                            roto_params, scoring_mode, pts_spec,
                            selected_cats = NULL) {
  orig_pa <- suppressWarnings(as.numeric(player_row[["pa"]]))
  has_orig <- is.finite(orig_pa) && orig_pa > 0

  # Merge projected PA into targets (deduplicated within 0.5 PA tolerance)
  if (has_orig) {
    already_there <- length(pa_targets) > 0 && any(abs(pa_targets - orig_pa) < 0.5)
    all_targets <- sort(if (already_there) pa_targets else c(pa_targets, orig_pa))
  } else {
    all_targets <- sort(pa_targets)
  }

  rows <- lapply(seq_along(all_targets), function(i) {
    res <- dlc_recalc_one_pa(player_row, all_targets[i], roto_params, scoring_mode, pts_spec)
    if (is.null(res)) return(NULL)
    list(data = res, is_proj = has_orig && abs(all_targets[i] - orig_pa) < 0.5)
  })
  rows <- Filter(Negate(is.null), rows)
  if (length(rows) == 0) return(NULL)

  is_proj <- vapply(rows, `[[`, logical(1L), "is_proj")
  mat <- do.call(rbind, lapply(rows, function(r) as.data.frame(t(r$data), stringsAsFactors = FALSE)))
  rownames(mat) <- NULL

  # Build col list from selected cats (filter to what recalc actually produced)
  if (scoring_mode == "roto") {
    cats       <- if (!is.null(selected_cats) && length(selected_cats) > 0)
                    selected_cats else c("pa","r","hr","rbi","sb","avg","obp")
    cats       <- unique(c("pa", cats))
    col_keys   <- c(cats, "z_total_s", "dollar_value")
    col_labels <- unname(sapply(col_keys, function(x) DLC_CAT_LABS[[x]] %||% x))
  } else {
    pts_cats   <- if (!is.null(pts_spec)) unique(c("pa", names(pts_spec))) else
                    c("pa","r","hr","rbi","sb","avg","obp")
    col_keys   <- c(pts_cats, "total_pts", "pts_per_g")
    col_labels <- unname(sapply(col_keys, function(x) DLC_CAT_LABS[[x]] %||% x))
  }

  keep <- col_keys[col_keys %in% names(mat)]
  labs <- col_labels[col_keys %in% names(mat)]

  table_rows <- lapply(seq_len(nrow(mat)), function(i) {
    cells <- lapply(seq_along(keep), function(j) {
      col <- keep[j]
      val <- mat[[col]][i]
      tags$td(dlc_fmt(suppressWarnings(as.numeric(val)), col))
    })
    if (is_proj[i]) tags$tr(class = "hypo-proj-row", cells)
    else            tags$tr(cells)
  })

  div(class = "table-responsive",
    tags$table(class = "table table-sm table-striped",
      tags$thead(tags$tr(lapply(labs, tags$th))),
      tags$tbody(table_rows)
    )
  )
}

# Build dropdown choices from a data frame — always alphabetical
dlc_choices <- function(df) {
  blank <- setNames("", "")
  if (is.null(df) || nrow(df) == 0) return(blank)
  df <- df[order(df$name), , drop = FALSE]
  labels <- paste0(df$name, " (", df$team, ")")
  c(blank, setNames(df$name, labels))
}

# Format a single cell value for the rank overview table.
# Mirrors dlc_fmt but treats sentinel 9999 as missing.
dlc_rank_fmt <- function(val, col) {
  if (col == "Player") return(as.character(val))
  v <- suppressWarnings(as.numeric(val))
  if (is.na(v) || v >= 9999) return("\u2014")
  as.character(as.integer(v))
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

dlCompareServer <- function(id, proj_results, adp_data, spr_data = reactive(NULL)) {
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
        nav_panel("Pitchers", div(style = "margin-top:16px;", uiOutput(ns("p_panel")))),
        nav_panel("Hypothetical",      div(style = "margin-top:16px;", uiOutput(ns("hypo_panel"))))
      )
    })

    # ── Hitter panel: DLC_MAX_COMPARE_SLOTS inputs (stacked) + table ──────────
    output$h_panel <- renderUI({
      hd <- h_data()
      ch <- dlc_choices(hd)
      tagList(
        div(class = "pag-panel",
          lapply(seq_len(DLC_MAX_COMPARE_SLOTS), function(i) {
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
      tryCatch({
        keys <- vapply(seq_len(DLC_MAX_COMPARE_SLOTS), function(i) {
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
        df <- df[!is.na(df$name), , drop = FALSE]
        # Sort by $Value (roto) or Total Pts (points), descending
        sort_col <- if (mode() == "roto") "dollar_value" else "total_pts"
        if (sort_col %in% names(df)) {
          df <- df[order(suppressWarnings(as.numeric(df[[sort_col]])), decreasing = TRUE,
                         na.last = TRUE), , drop = FALSE]
        }
        sel_h  <- tryCatch(proj_results$selected_cats_h(), error = function(e) NULL)
        pts_h  <- tryCatch(proj_results$pts_spec_h(),      error = function(e) NULL)
        cats_h <- if (mode() == "roto") sel_h else if (!is.null(pts_h)) names(pts_h) else NULL
        spec   <- dlc_col_spec(cats_h, is_hitter = TRUE, scoring_mode = mode())
        div(class = "pag-panel", dlc_player_table(df, spec$cols, spec$labels))
      }, error = function(e) {
        div(class = "pag-panel",
          p(style = "color:#c0392b; font-size:0.85rem; padding:12px 0;",
            tags$strong("Compare error (hitters): "), conditionMessage(e)))
      })
    })

    # ── Pitcher panel: DLC_MAX_COMPARE_SLOTS inputs (stacked) + table ─────────
    output$p_panel <- renderUI({
      pd <- p_data()
      ch <- dlc_choices(pd)
      tagList(
        div(class = "pag-panel",
          lapply(seq_len(DLC_MAX_COMPARE_SLOTS), function(i) {
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
        div(
          style = "margin-top:16px;",
          tags$p(class = "dl-howto-title", style = "margin-bottom:6px;", "Proj Stats"),
          uiOutput(ns("comp_p_table")),
          tags$p(class = "dl-howto-title", style = "margin-top:20px; margin-bottom:6px;", "Rank Overview"),
          uiOutput(ns("comp_p_rank_table"))
        )
      )
    })

    output$comp_p_table <- renderUI({
      tryCatch({
        keys <- vapply(seq_len(DLC_MAX_COMPARE_SLOTS), function(i) {
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
        df <- df[!is.na(df$name), , drop = FALSE]
        # Sort by ADP ascending (NA last), fallback to IP descending
        if (nrow(df) > 0) {
          if ("adp" %in% names(df)) {
            adp_v <- suppressWarnings(as.numeric(df$adp))
            ip_v  <- if ("ip" %in% names(df))
                       suppressWarnings(as.numeric(df$ip))
                     else rep(NA_real_, nrow(df))
            df <- df[order(is.na(adp_v), adp_v, -ip_v, na.last = TRUE), , drop = FALSE]
          } else if ("ip" %in% names(df)) {
            ip_v <- suppressWarnings(as.numeric(df$ip))
            df <- df[order(-ip_v, na.last = TRUE), , drop = FALSE]
          }
        }
        sel_p  <- tryCatch(proj_results$selected_cats_p(), error = function(e) NULL)
        pts_p  <- tryCatch(proj_results$pts_spec_p(),      error = function(e) NULL)
        cats_p <- if (mode() == "roto") sel_p else if (!is.null(pts_p)) names(pts_p) else NULL
        spec   <- dlc_col_spec(cats_p, is_hitter = FALSE, scoring_mode = mode())
        div(class = "pag-panel", dlc_player_table(df, spec$cols, spec$labels))
      }, error = function(e) {
        div(class = "pag-panel",
          p(style = "color:#c0392b; font-size:0.85rem; padding:12px 0;",
            tags$strong("Compare error (pitchers): "), conditionMessage(e)))
      })
    })

    output$comp_p_rank_table <- renderUI({
      keys <- vapply(seq_len(DLC_MAX_COMPARE_SLOTS), function(i) {
        v <- input[[paste0("sel_p_", i)]] %||% ""
        trimws(as.character(v))
      }, character(1L))
      keys <- unique(keys[nzchar(keys)])
      if (length(keys) == 0) return(NULL)

      spr <- tryCatch(spr_data(), error = function(e) NULL)
      if (is.null(spr) || nrow(spr) == 0)
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); font-size:0.82rem; padding:8px 0;",
            "SP Rank Overview data not yet loaded. Visit the SP Rank Overview tab first.")))

      # Normalise name key for matching
      nk_keys <- dlc_normalize_name(keys)
      spr$nk_tmp <- dlc_normalize_name(spr$Player)
      df_rank <- spr[spr$nk_tmp %in% nk_keys, , drop = FALSE]
      df_rank$nk_tmp <- NULL

      if (nrow(df_rank) == 0)
        return(div(class = "pf-empty",
          p(style = "color:var(--muted); font-size:0.82rem; padding:8px 0;",
            "No rank data found for selected pitchers.")))

      # Sort by Weighted Rank ascending (NA last)
      if ("Weighted Rank" %in% names(df_rank)) {
        wr <- suppressWarnings(as.numeric(df_rank[["Weighted Rank"]]))
        df_rank <- df_rank[order(is.na(wr), wr), , drop = FALSE]
      }

      # Display: Name, ADP Rank, Eno Rank, PitcherList Rank, SP Skillz Rank, Weighted Rank
      rank_cols <- c("Player", "ADP Rank", "Eno Rank", "PitcherList Rank",
                     "SP Skillz Rank", "Weighted Rank")
      rank_cols <- rank_cols[rank_cols %in% names(df_rank)]

      rows <- lapply(seq_len(nrow(df_rank)), function(i) {
        cells <- lapply(rank_cols, function(col) {
          tags$td(dlc_rank_fmt(df_rank[[col]][i], col))
        })
        tags$tr(cells)
      })

      div(class = "pag-panel",
        div(class = "table-responsive",
          tags$table(class = "table table-sm table-striped",
            tags$thead(tags$tr(lapply(rank_cols, tags$th))),
            tags$tbody(rows)
          )
        )
      )
    })

    # ── Hypothetical panel ────────────────────────────────────────────────────
    PA_DEFAULTS <- c(200, 300, 450, 500, 600)

    output$hypo_panel <- renderUI({
      hd <- h_data()
      ch <- dlc_choices(hd)

      # Isolate PA reads: these inputs are created BY this renderUI, so reading
      # them reactively creates a circular dependency that resets the player
      # selectize on every keystroke and breaks output$hypo_table.
      pa_vals <- vapply(seq_len(5), function(i) {
        v <- isolate(input[[paste0("hyp_pa_", i)]]) %||% PA_DEFAULTS[i]
        suppressWarnings(as.numeric(v))
      }, numeric(1L))

      tagList(
        div(class = "pag-panel",
          # Two player selectors side by side
          div(style = "display:flex; flex-wrap:wrap; gap:16px; margin-bottom:12px;",
            div(style = "flex:1; min-width:180px;",
              selectizeInput(
                ns("hypo_player"),
                label   = "Hitter 1",
                choices = ch,
                selected = isolate(input$hypo_player %||% ""),
                options = list(placeholder = "Select a hitter\u2026", create = FALSE,
                               selectOnTab = TRUE, closeAfterSelect = TRUE)
              )
            ),
            div(style = "flex:1; min-width:180px;",
              selectizeInput(
                ns("hypo_player_2"),
                label   = "Hitter 2 (optional)",
                choices = ch,
                selected = isolate(input$hypo_player_2 %||% ""),
                options = list(placeholder = "Select a hitter\u2026", create = FALSE,
                               selectOnTab = TRUE, closeAfterSelect = TRUE)
              )
            )
          ),
          tags$p(class = "dl-howto-title", style = "margin-bottom:6px;", "PA Thresholds"),
          div(
            style = "display:flex; flex-direction:column; gap:4px; max-width:200px;",
            lapply(seq_len(5), function(i) {
              numericInput(ns(paste0("hyp_pa_", i)), label = paste("PA", i),
                           value = pa_vals[i], min = 1, step = 10)
            })
          )
        ),
        div(style = "margin-top:16px;", uiOutput(ns("hypo_table")))
      )
    })

    output$hypo_table <- renderUI({
      tryCatch(hypo_table_inner(), error = function(e) {
        # Re-throw req() silent aborts so Shiny handles them normally (returns NULL)
        if (inherits(e, "shiny.silent.error")) stop(e)
        div(class = "pag-panel",
          p(style = "color:#c0392b; font-size:0.85rem; padding:12px 0;",
            tags$strong("Compare error (hypothetical): "), conditionMessage(e)))
      })
    })

    hypo_table_inner <- function() {
      req(input$hypo_player, nzchar(input$hypo_player))
      hd <- h_data()
      if (is.null(hd)) return(NULL)

      pa_targets <- vapply(seq_len(5), function(i) {
        suppressWarnings(as.numeric(input[[paste0("hyp_pa_", i)]] %||% PA_DEFAULTS[i]))
      }, numeric(1L))
      # Keep only valid (positive finite) thresholds
      pa_targets <- pa_targets[is.finite(pa_targets) & pa_targets > 0]
      if (length(pa_targets) == 0)
        return(div(class = "pf-empty", p("Enter at least one positive PA threshold.")))

      rp    <- tryCatch(proj_results$roto_params_h(),   error = function(e) NULL)
      ps    <- tryCatch(proj_results$pts_spec_h(),      error = function(e) NULL)
      sel_h <- tryCatch(proj_results$selected_cats_h(), error = function(e) NULL)

      # Build a scenario panel for one player \u2014 returns NULL if player not found
      make_hypo_panel <- function(player_name) {
        if (is.null(player_name) || !nzchar(player_name)) return(NULL)
        rows <- hd[hd$name == player_name, , drop = FALSE]
        if (nrow(rows) == 0) return(NULL)
        player_row <- rows[1, , drop = FALSE]
        tbl <- dlc_hypo_table(player_row, pa_targets, rp, mode(), ps,
                               selected_cats = sel_h)
        if (is.null(tbl)) return(NULL)
        div(
          tags$p(tags$strong(player_row$name),
                 tags$span(style = "color:var(--muted); font-size:0.82rem;",
                           paste0(" \u2014 projected PA: ", round(player_row$pa, 0)))),
          tbl
        )
      }

      p1 <- make_hypo_panel(input$hypo_player)
      if (is.null(p1))
        return(div(class = "pf-empty", p("Could not compute hypothetical scenarios.")))

      p2_name <- trimws(input$hypo_player_2 %||% "")
      p2 <- if (nzchar(p2_name)) make_hypo_panel(p2_name) else NULL

      if (!is.null(p2)) {
        div(class = "pag-panel",
          div(style = "display:flex; flex-wrap:wrap; gap:32px; align-items:flex-start;",
            div(style = "flex:1; min-width:280px; overflow-x:auto;", p1),
            div(style = "flex:1; min-width:280px; overflow-x:auto;", p2)
          )
        )
      } else {
        div(class = "pag-panel", p1)
      }
    }
  })
}
