suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(DT)
  library(rvest)
  library(ggplot2)
})

source("R/utils_names.R")          # shared player_nk() + override aliases
source("R/position_eligibility.R") # MLB position eligibility (GP-based)
source("R/sp_skillz.R")            # SP Skillz model (weights, reliability, scoring)
source("R/rp_skillz.R")            # RP Skillz model (velo, Stuff+, K%, CSW%, SD-MD, gmLI)
source("R/modules/mod_home.R")
source("R/modules/mod_park_factors.R")
source("R/modules/mod_sp_skillz.R")
source("R/modules/mod_rp_skillz.R")
source("R/modules/mod_proj_agg.R")
source("R/modules/mod_auc_val.R")
source("R/modules/mod_adp.R")
source("R/modules/mod_dl_compare.R")
source("R/modules/mod_sp_rank_overview.R")
source("R/modules/mod_team_importer.R")
source("R/modules/mod_draft_lab.R")
source("R/modules/mod_sp_streamonator.R")
source("R/modules/mod_hit_streamonator.R")
source("R/modules/mod_hit_dashboard.R")
source("R/modules/mod_pit_dashboard.R")
source("R/modules/mod_sp_outlook.R")
source("R/modules/mod_team_rater.R")
source("R/modules/mod_player_rater.R")
source("R/modules/mod_inseason_lab.R")
source("R/modules/mod_gsm.R")
source("R/modules/mod_research_hr_ev.R")
source("R/modules/mod_research_bat_speed.R")
source("R/modules/mod_research_csw.R")
source("R/modules/mod_research_hitter_whiff.R")
source("R/modules/mod_research_park_hr_barrel.R")
source("R/modules/mod_research_adj_barrel.R")
source("R/modules/mod_abrl_leaderboard.R")
source("R/modules/mod_park_calendar.R")

# ── Theme ─────────────────────────────────────────────────────────────────────

fbb_theme <- bs_theme(
  version      = 5,
  bg           = "#eef5ec",
  fg           = "#172733",
  primary      = "#2f7d3a",
  secondary    = "#b77343",
  base_font    = font_google("Manrope"),
  heading_font = font_google("Sora")
)

# ── CSS ───────────────────────────────────────────────────────────────────────

fbb_css <- "
:root {
  --bg-0:          #eef5ec;
  --bg-1:          #e1ecde;
  --bg-2:          #f9f8f3;
  --ink:           #172733;
  --muted:         #4a5a4f;
  --muted-light:   #7a8f7f;
  --line:          #c9d7c5;
  --card:          #ffffff;
  --primary:       #2f7d3a;
  --accent:        #b77343;
  --navy:          #1f3556;
  --primary-soft:  rgba(47,125,58,0.12);
  --accent-soft:   rgba(183,115,67,0.12);
  --navy-soft:     rgba(31,53,86,0.10);
  --shadow-sm:     0 1px 3px rgba(14,35,56,0.07), 0 1px 2px rgba(14,35,56,0.04);
  --shadow-md:     0 4px 16px rgba(14,35,56,0.08), 0 2px 4px rgba(14,35,56,0.04);
  --r-sm:          8px;
  --r-md:          12px;
  --r-lg:          16px;
}

/* ── Reset / Base ── */
html, body { background: var(--bg-0); }

/* ── Global input polish ── */
.form-control, .form-select, input[type=number], input[type=text] {
  border-color: var(--line) !important;
  border-radius: 7px !important;
  transition: border-color 0.15s, box-shadow 0.15s !important;
}
.form-control:focus, .form-select:focus,
input[type=number]:focus, input[type=text]:focus {
  border-color: var(--primary) !important;
  box-shadow: 0 0 0 3px rgba(47,125,58,0.13) !important;
  outline: none !important;
}
/* Checkboxes & radios — override Bootstrap blue */
.form-check-input:checked {
  background-color: var(--primary) !important;
  border-color: var(--primary) !important;
}
.form-check-input:focus {
  box-shadow: 0 0 0 3px rgba(47,125,58,0.18) !important;
  border-color: var(--primary) !important;
}
/* Selectize control polish */
.selectize-control .selectize-input {
  border-color: var(--line) !important;
  border-radius: 7px !important;
  box-shadow: none !important;
  transition: border-color 0.15s, box-shadow 0.15s !important;
}
.selectize-control.single .selectize-input.focus,
.selectize-control .selectize-input.focus {
  border-color: var(--primary) !important;
  box-shadow: 0 0 0 3px rgba(47,125,58,0.13) !important;
}
.selectize-dropdown {
  border: 1px solid rgba(14,35,56,0.08) !important;
  border-radius: 9px !important;
  box-shadow: 0 6px 20px rgba(14,35,56,0.10), 0 2px 6px rgba(14,35,56,0.06) !important;
  padding: 4px !important;
  overflow: hidden;
}
.selectize-dropdown-content .option {
  border-radius: 6px !important;
  padding: 8px 12px !important;
  font-size: 0.875rem !important;
  transition: background 0.1s !important;
}
.selectize-dropdown-content .option.active,
.selectize-dropdown-content .option:hover {
  background: var(--bg-0) !important;
  color: var(--primary) !important;
}
.selectize-dropdown-content .option.selected {
  background: var(--primary-soft) !important;
  color: var(--primary) !important;
  font-weight: 600 !important;
}

/* ── Navbar shell ── */
.navbar {
  background: linear-gradient(180deg, #3c8a48 0%, #276830 100%) !important;
  box-shadow: 0 1px 0 rgba(0,0,0,0.18), 0 2px 12px rgba(0,0,0,0.10) !important;
  padding-top: 0 !important;
  padding-bottom: 0 !important;
}
.navbar-brand {
  padding-top: 0 !important;
  padding-bottom: 4px !important;
  align-self: flex-end !important;
}
.navbar-nav .nav-link {
  font-size: 0.875rem !important;
  font-weight: 600 !important;
  letter-spacing: 0.01em !important;
  color: rgba(255,255,255,0.76) !important;
  padding: 18px 15px !important;
  border-bottom: 2px solid transparent !important;
  transition: color 0.15s, border-color 0.15s !important;
}
.navbar-nav .nav-link:hover,
.navbar-nav .nav-item.show > .nav-link {
  color: #fff !important;
  border-bottom-color: rgba(255,255,255,0.42) !important;
}
.navbar-nav .nav-link.active {
  color: #fff !important;
  font-weight: 700 !important;
  border-bottom-color: rgba(255,255,255,0.88) !important;
}
.nav-underline .nav-link.active { border-bottom: 2px solid rgba(255,255,255,0.88) !important; }
.navbar-nav .dropdown-toggle::after {
  opacity: 0.52;
  margin-left: 5px;
  vertical-align: 0.16em;
}
/* Dropdown menus */
.navbar .dropdown-menu {
  background: #fff !important;
  border: 1px solid rgba(14,35,56,0.08) !important;
  border-radius: 11px !important;
  box-shadow: 0 8px 28px rgba(14,35,56,0.12), 0 2px 8px rgba(14,35,56,0.07) !important;
  padding: 6px !important;
  margin-top: 5px !important;
  min-width: 215px !important;
}
.navbar .dropdown-item {
  border-radius: 7px !important;
  font-family: 'Manrope', sans-serif !important;
  font-size: 0.875rem !important;
  font-weight: 500 !important;
  color: var(--ink) !important;
  padding: 9px 13px !important;
  transition: background 0.1s, color 0.1s !important;
}
.navbar .dropdown-item:hover, .navbar .dropdown-item:focus {
  background: var(--bg-0) !important;
  color: var(--primary) !important;
  font-weight: 600 !important;
}
.navbar .dropdown-item.active, .navbar .dropdown-item:active {
  background: var(--primary-soft) !important;
  color: var(--primary) !important;
  font-weight: 600 !important;
}

/* ── Navbar Brand ── */
.brand-wrap {
  display: flex;
  align-items: center;
  cursor: pointer;
  overflow: visible;
}
.navbar-logo {
  height: 52px;
  width: auto;
  object-fit: contain;
  display: block;
}

/* ── Hero Logo ── */
.hero-logo-wrap {
  position: relative;
  display: inline-block;
  max-width: 560px;
  width: 88%;
  margin: 0 auto -28px;
}
.hero-logo {
  display: block;
  width: 100%;
}
.hero-est {
  position: absolute;
  bottom: 25%;
  right: 36%;
  transform: rotate(-27deg);
  transform-origin: center;
  font-family: 'Manrope', sans-serif;
  font-size: 0.62rem;
  font-weight: 500;
  font-style: italic;
  letter-spacing: 0.05em;
  color: var(--muted-light);
  pointer-events: none;
  white-space: nowrap;
}

/* ── Hero ── */
.home-hero {
  background:
    radial-gradient(960px 380px at 50% 115%, var(--accent-soft), transparent 64%),
    radial-gradient(1200px 500px at 8% -10%, var(--primary-soft), transparent 68%),
    radial-gradient(900px 400px at 94% 5%, var(--navy-soft), transparent 70%),
    linear-gradient(160deg, var(--bg-0) 0%, var(--bg-1) 55%, var(--bg-0) 100%);
  padding: 4px 24px 2px;
  text-align: center;
  border-bottom: 1px solid var(--line);
}
.hero-tagline {
  font-family: 'Manrope', sans-serif;
  font-size: 0.82rem;
  font-weight: 600;
  letter-spacing: 0.04em;
  color: var(--muted);
  margin: -10px auto 0;
  max-width: 560px;
  width: 88%;
  text-align: center;
}
.hero-dot {
  margin: 0 4px;
  color: var(--muted-light);
  font-weight: 400;
  font-size: 1.5rem;
  line-height: 0;
  vertical-align: middle;
}

/* ── Tools Section ── */
.home-tools-section {
  padding: 28px 32px 60px;
  max-width: 1100px;
  margin: 0 auto;
}
.tools-section-label {
  font-family: 'Sora', sans-serif;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  color: var(--muted-light);
  margin: 0 0 20px;
}

/* ── Home Accordions ── */
.home-accordion {
  --bs-accordion-bg: var(--card);
  --bs-accordion-border-color: var(--line);
  --bs-accordion-border-radius: var(--r-lg);
  border-radius: var(--r-lg);
  overflow: hidden;
}
.home-accordion .accordion-item {
  border: 1px solid var(--line);
  border-radius: var(--r-md) !important;
  margin-bottom: 12px;
  overflow: hidden;
  box-shadow: var(--shadow-sm);
  transition: box-shadow 0.2s ease;
}
.home-accordion .accordion-item:hover { box-shadow: var(--shadow-md); }
.home-accordion .accordion-button {
  padding: 16px 20px;
  background: var(--bg-1);
  border-left: 4px solid var(--line);
}
.home-accordion .accordion-button:not(.collapsed) {
  box-shadow: none;
  border-bottom: 1px solid var(--line);
}
.home-accordion .accordion-button::after { flex-shrink: 0; }
.home-accordion .accordion-body { padding: 0 !important; }

/* Category accent borders on accordion buttons */
.home-accordion .accordion-item:has([data-value='draft']) .accordion-button,
.home-accordion .accordion-item:has(.cat-card-body-draft) .accordion-button {
  border-left-color: var(--primary);
  background: linear-gradient(135deg, rgba(47,125,58,0.09) 0%, var(--bg-1) 100%);
}
.home-accordion .accordion-item:has(.cat-card-body-inseason) .accordion-button {
  border-left-color: var(--accent);
  background: linear-gradient(135deg, rgba(183,115,67,0.09) 0%, var(--bg-1) 100%);
}
.home-accordion .accordion-item:has(.cat-card-body-leaderboards) .accordion-button {
  border-left-color: var(--navy);
  background: linear-gradient(135deg, rgba(31,53,86,0.09) 0%, var(--bg-1) 100%);
}
.home-accordion .accordion-item:has(.cat-card-body-streamonators) .accordion-button {
  border-left-color: #5b3478;
  background: linear-gradient(135deg, rgba(91,52,120,0.09) 0%, var(--bg-1) 100%);
}
.home-accordion .accordion-item:has(.cat-card-body-methodology) .accordion-button {
  border-left-color: #c4782a;
  background: linear-gradient(135deg, rgba(196,120,42,0.09) 0%, var(--bg-1) 100%);
}
.home-accordion .accordion-item:has(.cat-card-body-research) .accordion-button {
  border-left-color: #8b2252;
  background: linear-gradient(135deg, rgba(139,34,82,0.09) 0%, var(--bg-1) 100%);
}

.cat-header-inner {
  display: flex;
  align-items: flex-start;
  gap: 14px;
}
.cat-emoji {
  font-size: 1.9rem;
  line-height: 1.15;
  flex-shrink: 0;
}
.cat-name {
  font-family: 'Sora', sans-serif;
  font-size: 1rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--ink);
  margin: 0 0 4px;
}
.cat-desc {
  font-size: 0.79rem;
  color: var(--muted);
  margin: 0;
  line-height: 1.4;
}
.cat-card-body { padding: 0 !important; }

/* ── Tool Rows ── */
.tool-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 13px 20px;
  min-height: 112px;
  border-bottom: 1px solid var(--line);
  gap: 16px;
  transition: background 0.12s ease;
}
.tool-row:last-child { border-bottom: none; }
.tool-row:hover { background: var(--bg-0); }
.tool-row-soon { opacity: 0.68; }
.tool-row-info { flex: 1; min-width: 0; }
.tool-row-name {
  font-weight: 650;
  font-size: 0.875rem;
  color: var(--ink);
  margin-bottom: 3px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tool-row-desc {
  font-size: 0.775rem;
  color: var(--muted);
  line-height: 1.35;
}
.tool-row-action { flex-shrink: 0; }

.btn-tool-open {
  background: linear-gradient(180deg, #379044 0%, #28692f 100%) !important;
  color: #fff !important;
  border: none !important;
  border-radius: 8px !important;
  font-size: 0.8rem !important;
  font-weight: 600 !important;
  padding: 6px 15px !important;
  line-height: 1.4 !important;
  box-shadow: 0 1px 3px rgba(40,105,47,0.22), 0 1px 2px rgba(40,105,47,0.12) !important;
  transition: box-shadow 0.15s, transform 0.1s !important;
  cursor: pointer;
  letter-spacing: 0.01em !important;
}
.btn-tool-open:hover {
  background: linear-gradient(180deg, #3e9e4c 0%, #2d7535 100%) !important;
  box-shadow: 0 3px 8px rgba(40,105,47,0.30), 0 1px 3px rgba(40,105,47,0.15) !important;
  transform: translateY(-1px) !important;
  color: #fff !important;
}
.btn-tool-open:active { transform: translateY(0) !important; }

.badge-soon {
  display: inline-block;
  background: #f2f2ee;
  color: #9a9a90;
  border: 1px solid #e2e1d8;
  border-radius: 20px;
  font-size: 0.67rem;
  font-weight: 600;
  padding: 4px 11px;
  white-space: nowrap;
  letter-spacing: 0.07em;
  text-transform: uppercase;
}

/* ── Coming-Soon Page ── */
.cs-page {
  min-height: 60vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
}
.cs-inner { text-align: center; max-width: 420px; }
.cs-emoji {
  font-size: 3.2rem;
  display: block;
  margin-bottom: 22px;
}
.cs-name {
  font-family: 'Sora', sans-serif;
  font-size: 1.65rem;
  font-weight: 700;
  letter-spacing: -0.035em;
  color: var(--ink);
  margin: 0 0 12px;
}
.cs-desc {
  font-size: 0.9rem;
  color: var(--muted);
  line-height: 1.55;
  margin: 0 0 26px;
}
.cs-badge {
  display: inline-block;
  background: var(--primary-soft);
  color: var(--primary);
  border: 1px solid rgba(47,125,58,0.28);
  border-radius: 20px;
  font-size: 0.75rem;
  font-weight: 700;
  padding: 5px 18px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

/* ── Shared page header (all tools) ── */
.pf-header-eyebrow,
.pag-breadcrumb {
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.13em;
  text-transform: uppercase;
  color: var(--muted-light);
  margin-bottom: 5px;
}
.pag-page-title {
  font-family: 'Sora', sans-serif;
  font-size: 1.75rem;
  font-weight: 800;
  letter-spacing: -0.035em;
  color: var(--ink);
  margin: 0 0 8px;
}
.pag-page-desc {
  font-size: 0.875rem;
  color: var(--muted);
  line-height: 1.55;
  margin: 0;
  max-width: 640px;
}
.pag-page-header { margin-bottom: 22px; }

/* ── Park Factors Page ── */
.pf-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 36px 32px 64px;
}
.pf-header { margin-bottom: 28px; }
.pf-title {
  font-family: 'Sora', sans-serif;
  font-size: 1.75rem;
  font-weight: 800;
  letter-spacing: -0.035em;
  color: var(--ink);
  margin: 0 0 10px;
}
.pf-subtitle {
  font-size: 0.875rem;
  color: var(--muted);
  line-height: 1.55;
  margin: 0;
  max-width: 640px;
}

/* Controls row */
.pf-controls-row {
  display: flex;
  align-items: center;
  gap: 24px;
  margin-bottom: 20px;
  flex-wrap: wrap;
}
.pf-control-group {
  display: flex;
  align-items: center;
  gap: 8px;
}
.pf-control-label {
  font-size: 0.78rem;
  font-weight: 600;
  color: var(--muted);
  white-space: nowrap;
  text-transform: uppercase;
  letter-spacing: 0.06em;
}
.pf-controls-spacer { flex: 1; }

/* Half toggle pill buttons — Shiny uses label.radio-inline > input + span */
.pf-toggle .shiny-input-container,
.pf-toggle .shiny-input-radiogroup { margin: 0; }
.pf-toggle .shiny-options-group { margin: 0; display: flex; gap: 0; }
.pf-toggle .radio-inline {
  padding: 0 !important;
  margin: 0 !important;
  display: flex;
  align-items: stretch;
}
.pf-toggle input[type='radio'] {
  position: absolute;
  opacity: 0;
  width: 0;
  height: 0;
  pointer-events: none;
}
.pf-toggle .radio-inline span {
  display: block;
  padding: 6px 16px;
  border: 1.5px solid var(--line);
  background: var(--card);
  color: var(--muted);
  cursor: pointer;
  font-size: 0.8rem;
  font-weight: 600;
  line-height: 1.5;
  border-right-width: 0;
  border-radius: 0;
  transition: background 0.12s, color 0.12s, border-color 0.12s;
  user-select: none;
  white-space: nowrap;
}
.pf-toggle .radio-inline:first-child span { border-radius: 8px 0 0 8px; }
.pf-toggle .radio-inline:last-child  span { border-radius: 0 8px 8px 0; border-right-width: 1.5px; }
.pf-toggle .radio-inline:only-child  span { border-radius: 8px; border-right-width: 1.5px; }
.pf-toggle .radio-inline span:hover { background: var(--bg-0); color: var(--ink); }
.pf-toggle input[type='radio']:checked + span {
  background: var(--primary) !important;
  border-color: var(--primary) !important;
  color: #fff !important;
  box-shadow: inset 0 1px 2px rgba(0,0,0,0.12) !important;
  font-weight: 700 !important;
}
/* Min sample select */
.pf-controls-row .selectize-control { margin: 0; }
.pf-controls-row .form-group { margin: 0; }
.pf-controls-row .selectize-input {
  background: rgba(255,255,255,0.78) !important;
}

/* Legend */
.pf-legend {
  display: flex;
  align-items: center;
  gap: 8px;
}
.pf-legend-label {
  font-size: 0.72rem;
  color: var(--muted);
  font-weight: 600;
  white-space: nowrap;
}
.pf-legend-bar {
  width: 160px;
  height: 10px;
  border-radius: 5px;
  flex-shrink: 0;
}

/* Table wrapper */
.pf-table-wrap {
  border: 1px solid var(--line);
  border-radius: var(--r-lg);
  overflow: hidden;
  box-shadow: var(--shadow-sm);
  background: var(--card);
}
/* DT table overrides */
.pf-dt.dataTable {
  width: 100% !important;
  border-collapse: collapse !important;
  font-family: 'Manrope', sans-serif;
  font-size: 0.875rem;
}
.pf-dt thead th {
  background: var(--bg-1) !important;
  color: var(--ink) !important;
  font-family: 'Sora', sans-serif !important;
  font-size: 0.72rem !important;
  font-weight: 700 !important;
  letter-spacing: 0.06em !important;
  text-transform: uppercase !important;
  border-bottom: 2px solid var(--line) !important;
  padding: 10px 12px !important;
  white-space: nowrap;
}
.pf-dt tbody td {
  border-bottom: 1px solid var(--line) !important;
  padding: 9px 12px !important;
  vertical-align: middle !important;
}
.pf-dt tbody tr:last-child td { border-bottom: none !important; }
/* Use inset box-shadow for hover — preserves DT inline background-color on colored cells */
.pf-dt tbody tr:hover td { box-shadow: inset 0 0 0 9999px rgba(47,125,58,0.05); }
.pf-dt tbody tr:hover td:nth-child(5),
.pf-dt tbody tr:hover td:nth-child(6),
.pf-dt tbody tr:hover td:nth-child(7) { box-shadow: inset 0 0 0 9999px rgba(0,0,0,0.06); }
/* Sort indicator color */
.pf-dt thead .sorting:after,
.pf-dt thead .sorting_asc:after,
.pf-dt thead .sorting_desc:after { color: var(--primary) !important; }


/* Footer */
.pf-footer {
  margin-top: 16px;
  padding: 0 4px;
}
.pf-footer-text {
  font-size: 0.75rem;
  color: var(--muted-light, #7a8f7f);
  line-height: 1.5;
}

/* ── ADP Page ── */
.adp-upload-row {
  display: flex;
  align-items: flex-end;
  gap: 12px;
  flex-wrap: wrap;
  margin-top: 14px;
  padding-top: 12px;
  border-top: 1px solid var(--line);
}
.adp-upload-row .form-group { margin-bottom: 0 !important; }
.adp-upload-row .adp-context-label { padding-bottom: 4px; }
.adp-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 36px 32px 64px;
}

/* Settings card */
.adp-settings-card {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 18px 22px 16px;
  margin-bottom: 8px;
}
.adp-settings-title {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 14px;
}
.adp-settings-row {
  display: flex;
  align-items: flex-end;
  gap: 20px;
  flex-wrap: wrap;
}
.adp-settings-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.adp-settings-btns {
  display: flex;
  align-items: center;
  gap: 10px;
  padding-bottom: 2px;
}
.adp-settings-card .form-group { margin-bottom: 0 !important; }
.adp-settings-card .selectize-input {
  background: rgba(255,255,255,0.78) !important;
}

/* Empty state */
.adp-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 60px 0 40px;
}
.adp-empty-inner {
  text-align: center;
  max-width: 380px;
}
.adp-empty-icon {
  font-size: 2.2rem;
  display: block;
  margin-bottom: 12px;
  opacity: 0.55;
}

/* Draft context row */
.adp-context-row {
  display: flex;
  align-items: flex-end;
  gap: 24px;
  padding: 14px 0 18px;
  flex-wrap: wrap;
}
.adp-context-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}
.adp-context-label {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--muted);
}
.adp-context-result .adp-pick-val {
  display: inline-block;
  font-family: 'Sora', sans-serif;
  font-size: 1.55rem;
  font-weight: 800;
  color: var(--primary);
  letter-spacing: -0.03em;
  line-height: 1;
  padding-top: 4px;
}
.adp-pick-na { color: var(--muted) !important; font-size: 1.2rem !important; }
.adp-context-row .form-group { margin-bottom: 0 !important; }
.adp-context-row input.form-control {
  width: 80px !important;
  text-align: center;
  font-weight: 600;
}

/* Position filter section */
.adp-pos-section {
  padding: 8px 0 16px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 12px;
}
.adp-pos-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 10px;
}
.adp-pos-btns {
  display: flex;
  gap: 0;
  border: 1px solid var(--line);
  border-radius: 8px;
  overflow: hidden;
  background: var(--bg-1);
}
.btn-adp-pos-quick {
  font-size: 0.76rem !important;
  font-weight: 600 !important;
  padding: 6px 14px !important;
  border-radius: 0 !important;
  border: none !important;
  border-right: 1px solid var(--line) !important;
  background: transparent !important;
  color: var(--muted) !important;
  transition: background 0.12s, color 0.12s !important;
  white-space: nowrap;
}
.btn-adp-pos-quick:last-child {
  border-right: none !important;
}
.btn-adp-pos-quick:hover {
  background: var(--primary-soft) !important;
  color: var(--primary) !important;
}
.btn-adp-pos-quick:focus {
  box-shadow: none !important;
  outline: none !important;
}
.adp-pos-checks .checkbox-inline { margin-right: 10px; }

/* Search row */
.adp-search-row { margin-bottom: 4px !important; }
.adp-search-wrap {
  max-width: 320px;
  width: 100%;
}

/* ── Draft Lab ─────────────────────────────────────────────────────────────── */
.dl-page {
  max-width: 1200px;
  margin: 0 auto;
  padding: 0 24px 48px;
}

/* Sub-tab nav pills: full-width flex, matching auc-mode-btn style */
.dl-page .nav-pills {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 28px;
  padding: 6px;
  background: var(--bg-1);
  border-radius: 12px;
  border: 1px solid var(--line);
}
.dl-page .nav-pills .nav-link {
  flex: 1;
  text-align: center;
  border-radius: 8px !important;
  font-family: 'Sora', sans-serif;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  padding: 8px 14px !important;
  color: var(--muted) !important;
  background: transparent !important;
  border: none !important;
  transition: all 0.15s;
  white-space: nowrap;
}
.dl-page .nav-pills .nav-link.active {
  background: var(--primary) !important;
  color: #fff !important;
  box-shadow: 0 2px 8px rgba(47,125,58,0.18);
}
.dl-tab-icon { margin-right: 5px; }

/* Vertical divider between tab groups — target the <li> created by nav_item() */
.dl-page .nav-pills > li:has(.dl-tab-divider) {
  display: flex;
  align-items: stretch;
  padding: 0;
  flex: 0 0 auto !important;
}
.dl-tab-divider {
  width: 1.5px;
  align-self: stretch;
  margin: 6px 4px;
  background: var(--muted);
  opacity: 0.35;
  border-radius: 1px;
}

/* ── Setup tab ── */
.dl-setup-page { display: flex; flex-direction: column; gap: 28px; }

.dl-status-row {
  display: flex;
  gap: 16px;
}
.dl-status-card {
  flex: 1;
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: var(--r-lg);
  padding: 20px 24px;
  box-shadow: var(--shadow-sm);
}
.dl-status-card--soon { opacity: 0.6; }
.dl-status-card-title {
  font-family: 'Sora', sans-serif;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 10px;
}
.dl-status-badge {
  display: inline-block;
  padding: 3px 11px;
  border-radius: 20px;
  font-size: 0.76rem;
  font-weight: 700;
  margin-bottom: 6px;
}
.dl-status-badge--none      { background: var(--bg-1); color: var(--muted); border: 1px solid var(--line); }
.dl-status-badge--generated { background: rgba(47,125,58,0.12); color: var(--primary); border: 1px solid rgba(47,125,58,0.25); }
.dl-status-badge--uploaded  { background: rgba(30,100,200,0.10); color: #1e64c8; border: 1px solid rgba(30,100,200,0.25); }
.dl-status-badge--error     { background: rgba(200,50,50,0.10); color: #c43232; border: 1px solid rgba(200,50,50,0.25); }
.dl-status-detail {
  font-size: 0.8rem;
  color: var(--muted);
  margin: 0;
}

.dl-upload-row {
  display: flex;
  gap: 16px;
}
.dl-upload-card {
  flex: 1;
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: var(--r-lg);
  padding: 20px 24px;
  box-shadow: var(--shadow-sm);
}
.dl-upload-card--soon { opacity: 0.6; }
.dl-upload-card-title {
  font-family: 'Sora', sans-serif;
  font-size: 0.78rem;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--ink);
  margin-bottom: 8px;
}
.dl-upload-instructions {
  font-size: 0.82rem;
  color: var(--muted);
  line-height: 1.5;
  margin-bottom: 14px;
}
.dl-upload-error {
  margin-top: 6px;
  font-size: 0.78rem;
  color: #c43232;
}
.dl-soon-note {
  font-size: 0.75rem;
  color: var(--muted);
  font-style: italic;
  margin-top: 8px;
}

.dl-howto {
  background: var(--bg-1);
  border: 1px solid var(--line);
  border-radius: var(--r-lg);
  padding: 24px;
  margin-bottom: 24px;
}
.dl-howto-title {
  font-family: 'Sora', sans-serif;
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 18px;
}
.dl-howto-steps { display: flex; gap: 24px; }
.dl-howto-step  { display: flex; gap: 14px; flex: 1; }
.dl-howto-step-num {
  width: 28px;
  height: 28px;
  min-width: 28px;
  background: var(--primary);
  color: #fff;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-family: 'Sora', sans-serif;
  font-weight: 700;
  font-size: 0.82rem;
  margin-top: 2px;
}
.dl-howto-step-body b  { font-size: 0.88rem; color: var(--ink); }
.dl-howto-step-body p  { font-size: 0.8rem; color: var(--muted); margin: 4px 0 0; line-height: 1.5; }
.dl-howto-detail { flex: 1; border: none; }
.dl-howto-summary {
  display: flex; align-items: center; gap: 10px;
  cursor: pointer; list-style: none; user-select: none;
  padding: 2px 0;
}
.dl-howto-summary::-webkit-details-marker { display: none; }
.dl-howto-summary > span { font-size: 0.88rem; font-weight: 600; color: var(--ink); }
.dl-howto-detail[open] .dl-howto-summary > span { color: var(--primary); }
.dl-howto-bullets {
  margin: 6px 0 0 38px; padding: 0;
  font-size: 0.79rem; color: var(--muted); line-height: 1.7;
}

/* ── Sub-tabs inside Draft Lab: hide standalone page headers ── */
.dl-adp-tab .pf-header,
.dl-proj-tab .pf-header,
.dl-spz-tab .pf-header { display: none !important; }

/* SP Rank Overview panel */
.spr-panel { padding: 16px 0; }
.spr-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 14px;
}
.spr-links {
  margin-top: 24px;
  padding-top: 14px;
  border-top: 1px solid var(--line);
  font-size: 0.85rem;
}
.spr-links ul {
  margin: 8px 0 0 0;
  padding-left: 18px;
}
.spr-links li { margin-bottom: 4px; }
.spr-weights { margin-bottom: 16px; }
.spr-wt-inputs {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  margin-top: 10px;
}
.spr-wt-field { display: flex; flex-direction: column; gap: 4px; }
.spr-wt-label {
  font-size: 0.75rem;
  font-weight: 600;
  color: var(--ink);
  white-space: nowrap;
}
.spr-wt-hint {
  font-size: 0.75rem;
  color: #666;
  margin: 8px 0 0 0;
}

/* ── Placeholder cards ── */
.dl-placeholder {
  min-height: 38vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
}
.dl-placeholder-inner {
  text-align: center;
  max-width: 420px;
}
.dl-placeholder-icon { font-size: 2.6rem; display: block; margin-bottom: 16px; }
.dl-placeholder-title {
  font-family: 'Sora', sans-serif;
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--ink);
  margin: 0 0 10px;
}
.dl-placeholder-desc {
  font-size: 0.88rem;
  color: var(--muted);
  line-height: 1.6;
  margin: 0 0 18px;
}
.dl-placeholder-badge {
  display: inline-block;
  padding: 4px 14px;
  border-radius: 20px;
  background: var(--bg-1);
  border: 1px solid var(--line);
  font-size: 0.74rem;
  font-weight: 700;
  color: var(--muted);
  letter-spacing: 0.05em;
  font-family: 'Sora', sans-serif;
}

/* ── SP Skillz Page ── */
.spz-page {
  max-width: 1100px;
  margin: 0 auto;
  padding: 36px 32px 64px;
}
/* Compact period toggle (8 buttons) */
.spz-period-toggle .radio-inline span {
  padding: 6px 11px;
  font-size: 0.76rem;
}
/* Search bar row */
.spz-search-row { margin-bottom: 4px !important; }
.spz-search-wrap {
  position: relative;
  width: 280px;
}
.spz-search-icon {
  position: absolute;
  left: 11px;
  top: 50%;
  transform: translateY(-50%);
  font-size: 1rem;
  color: var(--muted);
  pointer-events: none;
  z-index: 1;
  line-height: 1;
}
.spz-search-wrap .form-group { margin: 0; }
.spz-search-wrap input[type=text] {
  padding-left: 32px !important;
  height: 34px !important;
  font-size: 0.85rem !important;
  border-radius: 7px !important;
  border: 1px solid var(--line) !important;
  background: var(--card) !important;
  color: var(--ink) !important;
  box-shadow: none !important;
  transition: border-color 0.15s !important;
}
.spz-search-wrap input[type=text]:focus {
  border-color: var(--primary) !important;
  box-shadow: 0 0 0 3px rgba(47,125,58,0.10) !important;
  outline: none !important;
}
/* DT scroll header alignment */
.dataTables_scrollHeadInner { width: 100% !important; }
.dataTables_scrollHeadInner table { width: 100% !important; }
/* Fill the gap above the inner header table with the header bg color */
.pf-table-wrap .dataTables_scrollHead { background: var(--bg-1) !important; }
/* Sort arrow spacing — add a small gap between column name and arrow */
.pf-dt thead th.sorting::after,
.pf-dt thead th.sorting_asc::after,
.pf-dt thead th.sorting_desc::after { margin-left: 4px; }
/* Empty state */
.spz-empty {
  min-height: 38vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px 24px;
}
.spz-empty-inner { text-align: center; max-width: 420px; }
.spz-empty-title {
  font-family: 'Sora', sans-serif;
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--ink);
  margin: 0 0 10px;
}
.spz-empty-desc {
  font-size: 0.875rem;
  color: var(--muted);
  line-height: 1.55;
  margin: 0 0 20px;
}
/* ── SP Skillz Weights Card ── */
.spz-weights-card {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  padding: 16px 20px 18px;
  margin: 16px 0 4px;
}
.spz-weights-card-title {
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: var(--primary);
  margin-bottom: 4px;
}
.spz-weights-card-desc {
  font-size: 0.78rem;
  color: var(--muted);
  margin: 0 0 14px;
}
/* Preset buttons */
.spz-preset-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 10px 0 12px;
}
.spz-preset-label {
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  color: var(--muted);
}
.btn-spz-preset {
  font-size: 0.75rem;
  font-weight: 600;
  padding: 3px 12px;
  border-radius: 20px;
  border: 1.5px solid var(--line);
  background: transparent;
  color: var(--muted);
  transition: all 0.15s;
}
.btn-spz-preset:hover  { border-color: var(--primary); color: var(--primary); }
.btn-spz-preset-active { border-color: var(--primary) !important; color: var(--primary) !important; background: rgba(47,125,58,0.08) !important; }
/* Transposed weights table: rows = paradigms, columns = metrics */
.spz-weights-table-T {
  overflow-x: auto;
}
.spz-weights-table-T .spz-wt-row {
  display: grid;
  grid-template-columns: 76px repeat(8, 78px);
  align-items: center;
  column-gap: 4px;
  row-gap: 2px;
}
.spz-wt-header .spz-wt-col-label {
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.03em;
  color: var(--primary);
  text-align: center;
  padding-bottom: 4px;
  border-bottom: 1px solid var(--line);
}
.spz-wt-paradigm-label {
  font-size: 0.78rem;
  font-weight: 700;
  color: var(--text);
  line-height: 1.3;
  padding: 4px 0;
}
.spz-wt-paradigm-label small { font-weight: 400; color: var(--muted); }
.spz-wt-input .form-group { margin-bottom: 0; }
.spz-wt-input input { text-align: center; font-size: 0.82rem; padding: 3px 4px; }
.sps-diag-toggle { cursor: pointer; font-size: 0.82rem; color: var(--muted); }
/* ── RP Skillz weight row (7 metrics, single paradigm) ── */
.spz-weights-table-rp .rpz-wt-row {
  display: grid;
  grid-template-columns: 76px repeat(7, 84px);
  align-items: center;
  column-gap: 4px;
  row-gap: 2px;
}
/* ── SP Skillz Glossary ── */
.spz-glossary {
  margin-top: 28px;
  padding: 20px 24px;
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: var(--r-md);
  box-shadow: var(--shadow-sm);
}
.spz-glossary-title {
  font-family: 'Sora', sans-serif;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--muted-light);
  margin-bottom: 14px;
}
.spz-glossary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 14px 28px;
}
.spz-gloss-item { display: flex; flex-direction: column; gap: 3px; }
.spz-gloss-term {
  font-size: 0.79rem;
  font-weight: 700;
  color: var(--ink);
}
.spz-gloss-link { color: var(--primary) !important; text-decoration: none; }
.spz-gloss-link:hover { text-decoration: underline; }
.spz-gloss-def {
  font-size: 0.75rem;
  color: var(--muted);
  line-height: 1.45;
}

/* ── Projection Aggregator ──────────────────────────────────────────────────── */
.pag-page { padding: 0 0 48px; }
.pag-tab-body { padding: 20px 0 0; }

.pag-panel {
  background: #ffffff;
  border: 1px solid var(--line);
  border-radius: 11px;
  padding: 18px 20px 16px;
  box-shadow: var(--shadow-sm);
}
/* Side-by-side column header (Hitters / Starting Pitchers) */
.pag-col-header {
  font-size: 1.0rem;
  font-weight: 700;
  color: var(--ink);
  margin-bottom: 10px;
  padding-bottom: 6px;
  border-bottom: 2px solid var(--primary);
}
.pag-panel-title {
  font-size: 0.76rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--primary);
  margin-bottom: 4px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.pag-panel-subtitle {
  font-size: 0.74rem;
  color: #8a9a8f;
  margin-bottom: 12px;
}

/* System rows */
.pag-sys-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 7px 0;
  border-bottom: 1px solid #f0f5ee;
}
.pag-sys-row:last-child { border-bottom: none; }
.pag-sys-label { display: flex; align-items: center; gap: 8px; }
.pag-sys-name  { font-size: 0.88rem; font-weight: 600; color: #172733; }
.pag-sys-status { font-size: 0.71rem; color: #b0bdb4; font-style: italic; }
.pag-sys-unavail .pag-sys-name { color: #b0bdb4; }
.pag-sys-unavail .form-control { background: #f5f7f5 !important; color: #b0bdb4 !important; }
.pag-sys-input .form-control {
  width: 70px !important;
  text-align: center;
  border-color: #dde8da;
  font-size: 0.88rem;
}
.pag-sys-input .form-group,
.pag-sys-input .mb-3 { margin-bottom: 0 !important; }

/* Preset row */
.pag-preset-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 10px 0 6px;
}
.pag-preset-label {
  font-size: 0.72rem;
  font-weight: 700;
  color: #8a9a8f;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  white-space: nowrap;
}

/* Preset buttons */
.pag-preset-btns { display: flex; gap: 6px; margin-left: auto; }
.btn-pag-preset {
  font-size: 0.72rem;
  font-weight: 600;
  padding: 3px 11px;
  border-radius: 20px;
  background: var(--primary-soft);
  color: var(--primary);
  border: 1px solid rgba(47,125,58,0.22);
  transition: background 0.12s, box-shadow 0.12s;
  letter-spacing: 0.01em;
}
.btn-pag-preset:hover {
  background: rgba(47,125,58,0.20);
  color: var(--primary);
  box-shadow: 0 1px 4px rgba(47,125,58,0.18);
}

/* Category checkboxes */
.pag-panel .shiny-input-container { margin-bottom: 0; }
.pag-panel .checkbox-inline,
.pag-panel .form-check-inline {
  font-size: 0.83rem;
  color: #4a5a4f;
  margin-right: 4px;
}

/* Controls row (PT source + Generate) */
.pag-controls-row {
  display: flex;
  align-items: flex-end;
  gap: 14px;
  flex-wrap: wrap;
  margin: 16px 0 8px;
}
.pag-pt-source .form-group,
.pag-pt-source .mb-3 { margin-bottom: 0 !important; }
/* Highlighted PT source block */
.pag-pt-source--highlighted {
  background: #f7fbf8;
  border: 1px solid #b8d4be;
  border-radius: 8px;
  padding: 9px 13px 7px;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.pag-pt-source-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 0.78rem;
  font-weight: 600;
  color: var(--ink);
  margin-bottom: 1px;
}
.pag-pt-hint {
  font-size: 0.68rem; font-weight: 400; color: var(--muted);
  font-style: italic; margin: 0; line-height: 1.3;
}
/* PA / IP min inputs */
.pag-pa-min .form-group,
.pag-pa-min .mb-3,
.pag-ip-min .form-group,
.pag-ip-min .mb-3 { margin-bottom: 0 !important; }
.pag-pa-min label,
.pag-ip-min label { font-size: 0.75rem; font-weight: 600; color: #4a5a4f; }
.pag-pa-min input[type=number],
.pag-ip-min input[type=number] { width: 90px !important; }
.pag-generate-wrap { padding-bottom: 2px; }
.btn-pag-generate {
  background: linear-gradient(180deg, #274869 0%, #1b3252 100%);
  color: #fff !important;
  font-size: 0.84rem;
  font-weight: 700;
  padding: 8px 22px;
  border-radius: 8px;
  border: none;
  display: flex;
  align-items: center;
  gap: 7px;
  box-shadow: 0 1px 3px rgba(27,50,82,0.28), 0 1px 2px rgba(27,50,82,0.14);
  transition: box-shadow 0.15s, transform 0.1s;
  letter-spacing: 0.01em;
}
.btn-pag-generate:hover {
  background: linear-gradient(180deg, #2e5278 0%, #1f3a5e 100%);
  color: #fff !important;
  box-shadow: 0 3px 10px rgba(27,50,82,0.32), 0 1px 4px rgba(27,50,82,0.16);
  transform: translateY(-1px);
}
.btn-pag-generate:active { transform: translateY(0); }
.btn-pag-generate .fa { font-size: 0.75rem; }

/* Export row */
.pag-export-row { margin: 16px 0 10px; }
.btn-pag-export {
  background: linear-gradient(180deg, #379044 0%, #28692f 100%);
  color: #fff !important;
  font-size: 0.82rem;
  font-weight: 600;
  padding: 7px 18px;
  border-radius: 8px;
  border: none;
  box-shadow: 0 1px 3px rgba(40,105,47,0.22);
  transition: box-shadow 0.15s, transform 0.1s;
  letter-spacing: 0.01em;
}
.btn-pag-export:hover {
  background: linear-gradient(180deg, #3e9e4c 0%, #2d7535 100%);
  color: #fff !important;
  box-shadow: 0 3px 8px rgba(40,105,47,0.30);
  transform: translateY(-1px);
}
.btn-pag-export:active { transform: translateY(0); }

/* DT table title */
.pag-tbl-section-title {
  font-size: 0.95rem; font-weight: 700; color: var(--ink);
  letter-spacing: -0.01em; margin-bottom: 10px; padding-top: 4px;
}

/* DT controls bar (length + search) */
.pag-dt-controls {
  display: flex; align-items: center; justify-content: space-between;
  padding: 8px 2px 10px; gap: 12px;
}
.pag-dt-controls .dataTables_length,
.pag-dt-controls .dataTables_filter { margin: 0; }
.pag-dt-controls .dataTables_length label,
.pag-dt-controls .dataTables_filter label {
  display: flex; align-items: center; gap: 6px;
  font-size: 0.76rem; font-weight: 600; color: var(--muted);
  margin: 0; white-space: nowrap;
}
/* Length select */
.pag-dt-controls .dataTables_length select,
.pag-dt-controls .dataTables_length .form-select {
  border: 1px solid var(--line) !important; border-radius: 6px !important;
  padding: 3px 28px 3px 8px !important; font-size: 0.8rem !important;
  color: var(--ink) !important; background-color: #fff !important;
  height: auto !important; box-shadow: none !important;
}
/* Search input */
.pag-dt-controls .dataTables_filter input {
  border: 1px solid var(--line); border-radius: 7px;
  padding: 5px 11px; font-size: 0.82rem; background: #fff;
  color: var(--ink); outline: none; min-width: 200px;
  transition: border-color 0.15s, box-shadow 0.15s;
  box-shadow: none;
}
.pag-dt-controls .dataTables_filter input:focus {
  border-color: var(--primary);
  box-shadow: 0 0 0 2px rgba(47,125,58,0.12);
}

/* DT footer (info + pagination) */
.dataTables_wrapper .dataTables_info {
  font-size: 0.74rem; color: var(--muted-light); padding-top: 10px;
}
.dataTables_wrapper .dataTables_paginate { padding-top: 6px; }
.dataTables_wrapper .dataTables_paginate .page-link {
  font-size: 0.76rem; border-radius: 5px !important;
  padding: 3px 9px !important; color: var(--muted) !important;
  border: 1px solid transparent !important;
  background: transparent !important;
}
.dataTables_wrapper .dataTables_paginate .page-item.active .page-link,
.dataTables_wrapper .dataTables_paginate .page-item.active .page-link:hover {
  background: var(--primary) !important; color: #fff !important;
  border-color: var(--primary) !important;
}
.dataTables_wrapper .dataTables_paginate .page-item:not(.active):not(.disabled) .page-link:hover {
  background: var(--bg-1) !important; color: var(--ink) !important;
  border-color: var(--line) !important;
}

/* ── Auction Value Calculator ───────────────────────────────────────────── */

/* Scoring mode toggle bar */
.auc-mode-bar {
  display: flex;
  align-items: stretch;
  gap: 4px;
  background: var(--bg-1);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 5px;
  margin-bottom: 20px;
  box-shadow: inset 0 1px 4px rgba(14,35,56,0.09), inset 0 0 0 1px rgba(14,35,56,0.03);
}
.auc-mode-btn {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 3px;
  padding: 11px 20px;
  background: transparent;
  color: var(--muted);
  border: none;
  border-radius: 10px;
  cursor: pointer;
  transition: background 0.16s ease, color 0.16s ease, box-shadow 0.16s ease;
}
.auc-mode-btn:hover {
  color: var(--ink);
  background: rgba(255,255,255,0.55);
}
.auc-mode-btn .auc-mode-label {
  font-size: 0.97rem;
  font-weight: 700;
  letter-spacing: 0.01em;
  line-height: 1;
}
.auc-mode-btn .auc-mode-sub {
  font-size: 0.72rem;
  font-weight: 400;
  opacity: 0.70;
  line-height: 1;
}
.auc-mode-btn--active {
  background: var(--card);
  color: var(--primary);
  box-shadow: 0 1px 4px rgba(14,35,56,0.13), 0 0 0 1px rgba(14,35,56,0.06);
}
.auc-mode-btn--active .auc-mode-sub {
  color: var(--muted);
  opacity: 0.85;
}

/* Sections */
.auc-section {
  background: var(--card);
  border: 1px solid var(--line);
  border-radius: 11px;
  padding: 18px 20px 14px;
  margin-bottom: 16px;
  box-shadow: var(--shadow-sm);
}
.auc-section-title {
  font-size: 0.69rem; font-weight: 800; letter-spacing: 0.09em;
  text-transform: uppercase; color: var(--muted-light);
  margin-bottom: 12px;
}

/* Upload area */
.auc-upload-grid {
  display: grid; grid-template-columns: 1fr 1fr; gap: 16px;
  max-width: 680px; margin-bottom: 8px;
}
.auc-upload-col-title {
  display: block; font-size: 0.74rem; font-weight: 700;
  color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em;
  margin-bottom: 4px;
}
.auc-upload-hint {
  font-size: 0.72rem; color: var(--muted-light); margin-top: 4px; line-height: 1.4;
}

/* 2-column projection system grid */
.auc-sys-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  column-gap: 8px;
}
.auc-sys-grid .pag-sys-row {
  padding: 6px 0;
  border-bottom: 1px solid #f0f5ee;
}
.auc-sys-grid .pag-sys-row:nth-last-child(-n+2) { border-bottom: none; }

/* League settings row */
.auc-league-row {
  display: flex; flex-wrap: wrap; gap: 18px; align-items: flex-end;
}
.auc-league-field { display: flex; flex-direction: column; gap: 4px; }
.auc-field-label  { font-size: 0.74rem; font-weight: 600; color: var(--muted); }
.auc-budget-input {
  width: 90px !important;
  font-size: 0.88rem !important;
  padding: 5px 9px !important;
  border: 1px solid var(--line) !important;
  border-radius: 6px !important;
  -moz-appearance: textfield;
}
.auc-budget-input::-webkit-outer-spin-button,
.auc-budget-input::-webkit-inner-spin-button { -webkit-appearance: none; margin: 0; }
.auc-split-row { display: flex; align-items: center; gap: 6px; }
.auc-split-input { width: 60px !important; }
.auc-split-sep   { font-weight: 700; color: var(--muted); }
.auc-split-pct   { font-size: 0.78rem; color: var(--muted-light); white-space: nowrap; }

/* Points values grid */
.auc-pts-preset-row {
  display: flex; align-items: center; gap: 10px; margin-bottom: 14px;
}
.auc-pts-grid {
  display: flex; flex-wrap: wrap; gap: 10px 14px; margin-top: 8px;
}

/* Volume weight row (PA / IP) */
.auc-vol-weight-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 8px 0 4px;
  padding-bottom: 8px;
  border-bottom: 2px solid #d6e8d0;
}
.auc-vol-weight-label {
  font-size: 0.88rem;
  font-weight: 700;
  color: #2d6a4f;
  flex: 1;
}
.auc-vol-weight-row .shiny-input-container { margin-bottom: 0 !important; }
.auc-vol-weight-row input[type=number] { font-size: 0.85rem; padding: 2px 6px; height: 28px; }

/* Category weight grid */
.auc-cat-grid {
  display: flex;
  flex-direction: column;
  gap: 0;
  margin-top: 6px;
}
.auc-cat-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px solid #f0f5ee;
}
.auc-cat-row:last-child { border-bottom: none; }
.auc-cat-row > .form-group,
.auc-cat-row > .shiny-input-container { margin-bottom: 0 !important; flex: 1; }
.auc-cat-row > .form-group label { font-size: 0.88rem; font-weight: 600; color: #172733; margin-bottom: 0; }
.auc-cat-row input[type=number] { font-size: 0.85rem; padding: 2px 6px; height: 28px; }
.auc-pts-cell .form-group,
.auc-pts-cell .mb-3 { margin-bottom: 0 !important; }
.auc-pts-cell label { font-size: 0.72rem; font-weight: 600; color: var(--muted); }
.auc-pts-cell input[type=number] { width: 80px !important; font-size: 0.84rem !important; }

/* Action row */
.auc-action-row {
  display: flex; align-items: center; gap: 14px; margin: 4px 0 18px;
}

/* Error box */
.auc-error-box {
  background: #fff3f3; border: 1px solid #f5b8b8;
  border-radius: 8px; padding: 10px 14px;
  font-size: 0.82rem; color: #8b2020; margin-bottom: 12px;
  white-space: pre-wrap;
}

/* Hitter / Pitcher type switcher */
.pag-page .nav-pills {
  background: var(--bg-1);
  border: 1px solid var(--line);
  border-radius: 14px;
  padding: 5px;
  display: flex;
  gap: 4px;
  margin-bottom: 4px;
  box-shadow: inset 0 1px 4px rgba(14,35,56,0.09), inset 0 0 0 1px rgba(14,35,56,0.03);
}
.pag-page .nav-pills .nav-link {
  flex: 1;
  text-align: center;
  font-size: 0.97rem;
  font-weight: 700;
  color: var(--muted);
  border-radius: 10px;
  padding: 11px 20px;
  letter-spacing: 0.01em;
  transition: background 0.16s, color 0.16s, box-shadow 0.16s;
}
.pag-page .nav-pills .nav-link:hover:not(.active) {
  background: rgba(255,255,255,0.55);
  color: var(--ink);
}
.pag-page .nav-pills .nav-link.active {
  background: var(--card);
  color: var(--primary);
  font-weight: 700;
  box-shadow: 0 1px 4px rgba(14,35,56,0.13), 0 0 0 1px rgba(14,35,56,0.06);
}

/* ── Player Comparison ───────────────────────────────────────────────────── */
.pc-page { max-width: 1100px; }
.pc-selector-row { margin-bottom: 16px; }
.pc-comp-wrap { overflow-x: auto; }
.pc-comp-table { width: 100%; border-collapse: collapse; min-width: 400px; }
.pc-comp-table th.pc-stat-head {
  text-align: left; padding: 8px 14px; font-size: 0.74rem;
  font-weight: 700; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; border-bottom: 2px solid var(--line);
  white-space: nowrap;
}
.pc-comp-table th.pc-player-col {
  text-align: right; padding: 8px 14px; font-size: 0.82rem;
  font-weight: 700; color: var(--primary); border-bottom: 2px solid var(--primary);
  white-space: nowrap; min-width: 110px;
}
.pc-comp-table .pc-player-name { font-size: 0.86rem; font-weight: 700; }
.pc-comp-table .pc-player-team { font-size: 0.72rem; color: var(--muted); font-weight: 500; }
.pc-comp-table td.pc-stat-label {
  padding: 7px 14px; font-size: 0.8rem; font-weight: 600;
  color: var(--muted); white-space: nowrap;
}
.pc-comp-table td.pc-stat-val {
  text-align: right; padding: 7px 14px; font-size: 0.85rem;
  font-variant-numeric: tabular-nums; color: var(--ink);
}
.pc-comp-table tr { border-bottom: 1px solid var(--line); }
.pc-comp-table tr:last-child { border-bottom: none; }
.pc-comp-table tr.pc-sep-row td { border-top: 2px solid var(--line); }
.pc-comp-table td.pc-best-val {
  background: rgba(47,125,58,0.11); border-radius: 3px; font-weight: 700;
}
/* Hypothetical */
.hypo-proj-row td {
  font-weight: 700;
  background-color: rgba(30, 100, 60, 0.10) !important;
}
.pc-hypo-controls { display: flex; gap: 20px; flex-wrap: wrap; align-items: flex-end;
  padding-bottom: 8px; border-bottom: 1px solid var(--line); margin-bottom: 4px; }
.pc-hypo-type-row .shiny-input-container { margin-bottom: 0; }
.pc-hypo-select-row { flex: 1; min-width: 200px; }
.pc-hypo-card { min-height: 180px; }
.pc-hypo-title {
  font-size: 0.76rem; font-weight: 700; text-transform: uppercase;
  color: var(--muted); letter-spacing: 0.06em; margin-bottom: 10px;
  display: flex; align-items: center; gap: 8px;
}
.pc-hypo-pt-badge {
  font-size: 0.7rem; font-weight: 600; background: var(--primary);
  color: #fff; padding: 2px 8px; border-radius: 20px; letter-spacing: 0;
  text-transform: none;
}
.pc-hypo-stat-list { display: flex; flex-direction: column; gap: 2px; }
.pc-hypo-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 5px 0; border-bottom: 1px solid rgba(0,0,0,0.05);
}
.pc-hypo-row.pc-hypo-sep { border-top: 2px solid var(--line); margin-top: 4px; padding-top: 7px; }
.pc-hypo-label { font-size: 0.78rem; font-weight: 600; color: var(--muted); }
.pc-hypo-val { font-size: 0.85rem; font-variant-numeric: tabular-nums; color: var(--ink); }
.pc-hypo-val.pc-hypo-changed { color: var(--primary); font-weight: 700; }
/* No-results banner */
.pc-no-results { text-align: center; padding: 50px 20px; color: var(--muted); }
.pc-no-results-icon { font-size: 2.6rem; margin-bottom: 12px; }
.pc-no-results h4 { font-size: 1rem; font-weight: 700; margin-bottom: 8px; color: var(--ink); }
.pc-no-results p { font-size: 0.86rem; margin-bottom: 18px; max-width: 420px; margin-inline: auto; }

/* ── Team Importer ───────────────────────────────────────────────────────── */
.ti-page { max-width: 1200px; }
.ti-card-title {
  font-size: 0.74rem; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.07em; color: var(--muted); margin-bottom: 12px;
}
.ti-setup-card, .ti-io-card, .ti-roster-card, .ti-proj-card, .ti-tgt-card {
  margin-bottom: 16px;
}
.ti-divider { border-color: var(--line); margin: 12px 0; }
.ti-league-row { display: flex; gap: 12px; }
.ti-league-field { flex: 1; }
/* Auto benchmark label */
.ti-benchmark-label-wrap { margin-bottom: 12px; }
.ti-benchmark-label-wrap .auc-field-label { display: block; margin-bottom: 4px; }
.ti-benchmark-val {
  font-family: 'Helvetica Neue', Arial, sans-serif;
  font-size: 0.92rem; font-weight: 600; color: #172733;
}
.ti-benchmark-tm { font-weight: 400; color: var(--muted, #888); margin-left: 4px; }
.ti-preset-row { margin-top: 6px; }
.ti-preset-btn { width: 100%; }
/* Slot counts */
.ti-slot-counts { display: flex; flex-direction: column; gap: 2px; }
.ti-count-row {
  display: flex; justify-content: space-between; align-items: center;
  padding: 3px 0;
}
.ti-count-label {
  font-size: 0.82rem; font-weight: 600; color: var(--ink); min-width: 60px;
}
.ti-count-row .shiny-input-container { margin-bottom: 0; }
/* Slot header */
.ti-slot-header {
  display: flex; align-items: center; padding: 0 0 6px 0;
  border-bottom: 2px solid var(--primary); margin-bottom: 4px;
  font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.06em; color: var(--muted);
}
/* Slot row */
.ti-slot-wrap { display: contents; }
.ti-slot-row {
  display: flex; align-items: center; gap: 8px;
  padding: 3px 0; border-bottom: 1px solid var(--line);
}
.ti-slot-row:last-child { border-bottom: none; }
.ti-slot-pos { width: 52px; flex-shrink: 0; }
.ti-pos-badge {
  display: inline-block; font-size: 0.7rem; font-weight: 700;
  background: var(--primary); color: #fff;
  padding: 2px 7px; border-radius: 10px; white-space: nowrap;
}
.ti-slot-player { flex: 1; min-width: 0; }
.ti-slot-player .shiny-input-container,
.ti-slot-round .shiny-input-container { margin-bottom: 0; }
.ti-slot-round { width: 72px; flex-shrink: 0; }
.ti-slot-overall {
  width: 48px; flex-shrink: 0; text-align: right;
  font-size: 0.82rem; font-variant-numeric: tabular-nums;
  color: var(--muted); font-weight: 600;
}
/* Position group */
.ti-pos-group { margin-bottom: 12px; }
.ti-pos-group-label {
  font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.07em; color: var(--muted); margin: 8px 0 4px;
  padding-bottom: 3px; border-bottom: 1px solid var(--line);
}
/* Projected stats table */
.ti-proj-scroll { overflow-x: auto; }
.ti-proj-table {
  width: 100%; border-collapse: collapse; min-width: 420px;
  font-size: 0.82rem;
}
.ti-proj-table th.ti-pt-name-h {
  text-align: left; padding: 6px 10px; font-size: 0.72rem;
  font-weight: 700; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; border-bottom: 2px solid var(--line);
}
.ti-proj-table th.ti-pt-stat-h {
  text-align: right; padding: 6px 10px; font-size: 0.72rem;
  font-weight: 700; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; border-bottom: 2px solid var(--line);
  white-space: nowrap;
}
.ti-proj-table td.ti-pt-name {
  padding: 5px 10px; font-weight: 600; color: var(--ink);
  white-space: nowrap;
}
.ti-proj-table td.ti-pt-stat {
  text-align: right; padding: 5px 10px;
  font-variant-numeric: tabular-nums; color: var(--ink);
}
.ti-proj-table tr { border-bottom: 1px solid var(--line); }
.ti-proj-table tr:last-child { border-bottom: none; }
/* Target comparison */
.ti-tgt-grid { display: flex; gap: 24px; flex-wrap: wrap; }
.ti-tgt-block { flex: 1; min-width: 240px; }
.ti-tgt-title {
  font-size: 0.74rem; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.06em; color: var(--muted); margin-bottom: 8px;
}
.ti-tgt-table { width: 100%; border-collapse: collapse; font-size: 0.83rem; }
.ti-tgt-table th {
  text-align: right; padding: 5px 10px; font-size: 0.72rem;
  font-weight: 700; color: var(--muted); text-transform: uppercase;
  letter-spacing: 0.06em; border-bottom: 2px solid var(--line);
}
.ti-tgt-table th:first-child { text-align: left; }
.ti-tgt-table td { padding: 5px 10px; border-bottom: 1px solid var(--line); }
.ti-tgt-table tr:last-child td { border-bottom: none; }
.ti-tgt-cat { font-weight: 600; color: var(--muted); }
.ti-tgt-team, .ti-tgt-tgt, .ti-tgt-per,
.ti-tgt-team-per { text-align: right; font-variant-numeric: tabular-nums; }
.ti-tgt-per-h { text-align: right; }
.ti-tgt-team-per { color: var(--muted); }
/* Target table color coding: ±5% window, green = meeting/exceeding, red = below */
.ti-tgt-good { background: rgba(47, 125, 58, 0.12); border-radius: 3px; }
.ti-tgt-bad  { background: rgba(180, 35, 35, 0.10); border-radius: 3px; }
/* Auction value rank column (#) */
.auc-rank-col { color: var(--muted) !important; font-size: 0.78rem !important; font-weight: 600 !important; }
/* Bench separator row inside the unified projection table */
.ti-proj-table tr.ti-bench-sep-row { border-top: 2px dashed var(--line); border-bottom: none; }
.ti-proj-table td.ti-bench-sep-cell {
  font-size: 0.68rem; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.07em; color: var(--muted);
  padding: 6px 10px 3px; text-align: left;
}
.ti-tgt-gap {
  text-align: right; font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.ti-gap-good { color: #2f7d3a; }
.ti-gap-bad  { color: #b83232; }
.ti-gap-neutral { color: var(--muted); }
/* Import/export */
.ti-io-buttons { display: flex; gap: 10px; align-items: flex-start; flex-wrap: wrap; }
.ti-import-wrap { flex: 1; min-width: 160px; }
.ti-import-wrap .shiny-input-container { margin-bottom: 0; }
/* No-proj banner */
.ti-no-proj {
  text-align: center; padding: 60px 20px; color: var(--muted);
}
.ti-no-proj-icon { font-size: 2.8rem; margin-bottom: 14px; }
.ti-no-proj h4 { font-size: 1rem; font-weight: 700; color: var(--ink); margin-bottom: 8px; }
.ti-no-proj p  { font-size: 0.86rem; margin-bottom: 18px; max-width: 400px; margin-inline: auto; }
.ti-empty { color: var(--muted); font-size: 0.85rem; padding: 12px 0; }

/* ── Team Rater ── */
.tr-page { padding: 0 0 48px; }
.tr-note { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }
.tr-ha-badge {
  display: inline-flex; align-items: center; justify-content: center;
  width: 16px; height: 16px; border-radius: 50%;
  font-size: 0.65rem; font-weight: 800; line-height: 1;
}
.tr-ha-l { background: #1f3556; color: #fff; }
.tr-ha-r { background: var(--accent); color: #fff; }

/* ── SP Streamonator ── */
.sps-page { padding: 0 0 48px; }
.sps-week-label { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }
/* Weights panel — mirrors pag-panel style */
.sps-weights-wrap { display: flex; gap: 16px; flex-wrap: wrap; margin: 12px 0 0; }
.sps-weights-panel {
  background: #ffffff;
  border: 1px solid var(--line);
  border-radius: 11px;
  padding: 18px 20px 16px;
  box-shadow: var(--shadow-sm);
  flex: 0 0 auto;
}
.sps-weights-panel-title {
  font-size: 0.76rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--ink);
  margin-bottom: 2px;
}
.sps-weights-panel-subtitle { font-size: 0.74rem; color: #8a9a8f; margin-bottom: 14px; }
.sps-weight-row { display: flex; gap: 20px; align-items: flex-end; flex-wrap: wrap; margin-bottom: 10px; }
.sps-weight-row-inner { gap: 12px; margin-top: 6px; }
.sps-weight-row-label { font-size: 0.78rem; font-weight: 600; color: var(--muted); align-self: center; padding-bottom: 4px; white-space: nowrap; }
.sps-weight-item { display: flex; flex-direction: column; align-items: center; gap: 4px; }
.sps-weight-label { font-size: 0.78rem; font-weight: 600; color: var(--ink); white-space: nowrap; }
.sps-weight-divider { width: 1px; height: 64px; background: var(--line); align-self: center; margin: 0 4px; }
.sps-weight-group { display: flex; flex-direction: column; align-items: flex-start; }
.sps-weight-group-label {
  font-size: 0.7rem; font-weight: 700; letter-spacing: 0.08em; text-transform: uppercase;
  color: var(--primary); border-bottom: 2px solid var(--primary);
  padding-bottom: 3px; margin-bottom: 2px; align-self: stretch; text-align: center;
}
/* Fetch row — sits between weights card and the pill tabs */
.sps-fetch-row { display: flex; align-items: center; gap: 14px; margin: 16px 0 8px; }
.sps-susp-note { margin-top: 8px; font-size: 0.78rem; color: #8a9a8f; font-style: italic; }
.sps-susp-dagger { color: #b07a2a; font-weight: 700; font-style: normal; }
.sps-status { font-size: 0.78rem; color: var(--muted); }
/* Tab body */
.sps-tab-body { margin-top: 16px; }
.sps-empty { padding: 40px 0; text-align: center; color: var(--muted); font-size: 0.9rem; }
/* FAAB Helper — enrichment badges */
.faab-badges-row { display: flex; gap: 8px; flex-wrap: wrap; margin: 4px 0 12px; }
.faab-badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; letter-spacing: 0.02em; }
.faab-badge-ok { background: var(--primary-soft); color: var(--primary); border: 1px solid rgba(47,125,58,0.25); }
.faab-badge-na { background: var(--bg-1); color: var(--muted-light); border: 1px solid var(--line); }
/* FAAB Helper — filter rows */
.faab-filter-row {
  display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  margin-bottom: 10px;
}
.faab-filter-row .shiny-input-container { margin-bottom: 0; }
.faab-pos-filter-row {
  display: flex; align-items: center; gap: 6px; flex-wrap: wrap;
  margin-bottom: 10px;
}
.faab-pos-filter-row .shiny-input-container { margin-bottom: 0; }
.faab-pos-filter-row .checkbox-inline,
.faab-pos-filter-row .form-check-inline { margin-right: 2px; }
.faab-pos-filter-row .checkbox-inline label,
.faab-pos-filter-row .form-check-inline label,
.faab-pos-filter-row .form-check label { font-size: 0.8rem; color: var(--ink); }
/* Day-of-week filter row */
.sps-day-filter-row {
  display: flex; align-items: center; gap: 10px; flex-wrap: wrap;
  margin-bottom: 10px;
}
.sps-day-filter-row .shiny-input-container { margin-bottom: 0; }
.sps-day-filter-row .checkbox-inline,
.sps-day-filter-row .form-check-inline { margin-right: 2px; }
.sps-day-filter-row .checkbox-inline label,
.sps-day-filter-row .form-check-inline label,
.sps-day-filter-row .form-check label { font-size: 0.8rem; color: var(--ink); }
.btn-sps-day {
  font-size: 0.72rem; font-weight: 600; padding: 3px 10px;
  border-radius: 20px; border: 1.5px solid var(--line);
  background: transparent; color: var(--muted);
  line-height: 1.4; cursor: pointer;
}
.btn-sps-day:hover { border-color: var(--primary); color: var(--primary); }
/* Search bar: match the pf-dt / SP Skillz input style */
.sps-tab-body .dataTables_filter {
  margin-bottom: 10px;
  text-align: left;
}
.sps-tab-body .dataTables_filter label {
  font-size: 0.8rem; color: var(--muted);
  display: inline-flex; align-items: center; gap: 8px;
}
.sps-tab-body .dataTables_filter input {
  height: 32px; padding: 4px 12px;
  font-size: 0.8rem; font-family: 'Manrope', sans-serif;
  border: 1.5px solid var(--line); border-radius: 20px;
  background: var(--bg-0); color: var(--ink);
  outline: none; transition: border-color .15s;
}
.sps-tab-body .dataTables_filter input:focus { border-color: var(--primary); }
/* My Pitchers */
/* My Pitchers tab — new slot-based layout */
.sps-my-wrap2 { padding: 4px 0 8px; }
.sps-my-header { display: flex; align-items: flex-end; gap: 16px; flex-wrap: wrap; margin-bottom: 14px; }
.sps-my-header .form-group { margin-bottom: 0; }
.sps-my-header label { white-space: nowrap; }
.sps-my-io { display: flex; align-items: center; gap: 8px; padding-bottom: 2px; }
.sps-my-io .btn { font-size: 0.75rem; padding: 3px 11px; border-radius: 20px; }
.sps-my-slots {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 18px;
}
.sps-my-slot { width: 178px; }
.sps-my-slot .form-group { margin-bottom: 0; }

/* ── Status shell ── */
.status-shell {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 0.82rem;
  color: var(--muted);
  min-height: 1.4em;
}
.status-ok   { color: #2d6a4f; }
.status-warn { color: #e67e22; }
.status-error{ color: #c0392b; }

/* ── Wizard stepper ── */
.wiz-steps {
  display: flex;
  align-items: center;
  gap: 0;
  margin-bottom: 24px;
  user-select: none;
}
.wiz-step-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  font: inherit;
}
.wiz-step-btn:disabled { cursor: default; }
.wiz-circle {
  width: 32px; height: 32px;
  border-radius: 50%;
  display: inline-flex; align-items: center; justify-content: center;
  font-size: 0.85rem; font-weight: 700;
  border: 2px solid var(--line);
  background: var(--card);
  color: var(--muted);
  flex-shrink: 0;
  transition: background 0.15s, border-color 0.15s, color 0.15s;
}
.wiz-circle.active  { background: var(--primary); border-color: var(--primary); color: #fff; }
.wiz-circle.done    { background: var(--primary-soft); border-color: var(--primary); color: var(--primary); }
.wiz-label {
  font-size: 0.82rem; font-weight: 600;
  color: var(--muted);
  white-space: nowrap;
}
.wiz-label.active { color: var(--primary); }
.wiz-label.done   { color: var(--muted); }
.wiz-connector {
  flex: 1; min-width: 24px; max-width: 60px;
  height: 2px;
  background: var(--line);
  margin: 0 8px;
  transition: background 0.15s;
}
.wiz-connector.done { background: var(--primary); }
.wiz-nav {
  display: flex;
  gap: 10px;
  margin-top: 24px;
  padding-top: 16px;
  border-top: 1px solid var(--line);
}

/* ── Inline-flex helper for auc-val playing-time row ── */
.auc-pt-row {
  display: flex;
  gap: 16px;
  align-items: flex-end;
}
.auc-pt-row-main { flex: 1; }

/* ── SP Outlook cells ────────────────────────────────────────────────────── */
.spo-cell {
  display: flex; flex-direction: column; align-items: center; gap: 2px;
  padding: 4px 0; line-height: 1.35; text-align: center;
}
.spo-ha   { font-weight: 600; font-size: 0.88rem; color: #172733; }
.spo-date { font-size: 0.80rem; color: #4a5a4f; }
.spo-meta { font-size: 0.80rem; color: #4a6a50; font-weight: 500; letter-spacing: 0.01em; }
/* Projected starts: subtler treatment */
.spo-proj .spo-ha   { color: #8a9e90; font-weight: 500; }
.spo-proj .spo-date { color: #9aac9f; }
.spo-proj .spo-meta { color: #9aac9f; }
.spo-na { color: #c0c8c3; font-size: 0.85rem; }
/* SP Outlook legend */
.spo-legend {
  display: flex; align-items: center; gap: 1.4rem;
  margin: 0 0 0.75rem; font-family: 'Helvetica Neue', Arial, sans-serif;
}
.spo-legend-item { display: flex; align-items: center; gap: 0.4rem; }
.spo-legend-swatch {
  font-size: 0.82rem; font-weight: 600; letter-spacing: 0.02em;
}
.spo-legend-confirmed { color: #172733; }
.spo-legend-projected { color: #8a9e90; font-weight: 500; }
.spo-legend-label     { font-size: 0.82rem; color: var(--muted, #888); }

/* ═══════════════════════════════════════════════════════════
   MOBILE RESPONSIVENESS  (max-width breakpoints)
   ═══════════════════════════════════════════════════════════ */

/* ── 768px and below ─────────────────────────────────────── */
@media (max-width: 768px) {

  /* Prevent iOS auto-zoom on input focus */
  body { font-size: 16px; }
  input, select, textarea { font-size: 16px !important; }

  /* Reduce page padding on all content pages */
  .pf-page, .adp-page, .spz-page, .sps-page,
  .dl-page, .pag-page, .auc-page, .cs-page {
    padding: 20px 14px !important;
  }

  /* Navbar dropdown width */
  .navbar .dropdown-menu { min-width: min(215px, 90vw); }

  /* Tab pill rows — horizontal scroll instead of wrapping */
  .dl-page .nav-pills,
  .pag-page .nav-pills {
    flex-wrap: nowrap !important;
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
    scrollbar-width: none;
    padding-bottom: 4px;
  }
  .dl-page .nav-pills::-webkit-scrollbar,
  .pag-page .nav-pills::-webkit-scrollbar { display: none; }

  /* SP Streamonator — weights panel stacks */
  .sps-weights-panel {
    flex-direction: column !important;
    width: 100% !important;
  }
  .sps-weights-panel > * { width: 100% !important; }

  /* SP Streamonator — my pitchers header stacks */
  .sps-my-header {
    flex-direction: column !important;
    align-items: flex-start !important;
    gap: 12px !important;
  }

  /* SP Streamonator — pitcher slots 2-per-row on tablet */
  .sps-my-slot { width: calc(50% - 8px) !important; }

  /* SP Skillz search wrap */
  .spz-search-wrap { width: 100%; max-width: 100%; }

  /* SP Skillz weights table — allow horizontal scroll */
  .spz-weights-table-T { overflow-x: auto; -webkit-overflow-scrolling: touch; }

  /* Park Factors controls row */
  .pag-controls-row { flex-wrap: wrap; gap: 10px; }

  /* ADP playing-time row */
  .auc-pt-row { flex-wrap: wrap; gap: 10px; }

  /* DataTable search bars — full width */
  .dataTables_filter { width: 100% !important; }
  .dataTables_filter input { width: 100% !important; box-sizing: border-box; }

  /* DataTables — reduce table scrollY minimum */
  .dataTables_scrollBody { min-height: 200px; }

  /* Day-of-week filter row wraps nicely */
  .sps-day-filter-row {
    flex-wrap: wrap;
    gap: 6px;
  }
  .sps-day-filter-row .shiny-input-checkboxgroup { flex-wrap: wrap; }
}

/* ── 480px and below ─────────────────────────────────────── */
@media (max-width: 480px) {

  /* Extra padding reduction */
  .pf-page, .adp-page, .spz-page, .sps-page,
  .dl-page, .pag-page, .auc-page, .cs-page {
    padding: 14px 10px !important;
  }

  /* SP Streamonator — pitcher slots full-width on phone */
  .sps-my-slot { width: 100% !important; }

  /* Brand logo — slightly smaller */
  .navbar-logo { height: 28px !important; }

  /* Wizard nav buttons stack */
  .wiz-nav { flex-direction: column; }
  .wiz-nav .btn { width: 100%; }

  /* Nav pills in modals / sub-tabs */
  .nav-pills .nav-link { padding: 6px 10px; font-size: 0.82rem; }

  /* Import/export button row wraps */
  .sps-my-io { flex-wrap: wrap; gap: 6px; }
  .sps-my-io .btn { flex: 1 1 auto; }
}
"

# ── Helpers ───────────────────────────────────────────────────────────────────

coming_soon <- function(tool_name, desc = NULL) {
  div(
    class = "cs-page",
    div(
      class = "cs-inner",
      tags$span(class = "cs-emoji", "\U0001F6A7"),
      h2(class = "cs-name", tool_name),
      if (!is.null(desc)) p(class = "cs-desc", desc),
      tags$span(class = "cs-badge", "Coming Soon")
    )
  )
}

# ── UI ────────────────────────────────────────────────────────────────────────

ui <- page_navbar(
  id           = "main_nav",
  title        = div(
    class   = "brand-wrap",
    onclick = "Shiny.setInputValue('brand_click', Math.random());",
    tags$img(src = "logo_collinmyshot.png", class = "navbar-logo", alt = "Collinmyshot")
  ),
  theme        = fbb_theme,
  window_title = "Collinmyshot Fantasy Baseball",
  bg           = "#2f7d3a",
  inverse      = TRUE,
  header       = tags$head(
    tags$link(rel = "icon", type = "image/png", href = "favicon.png"),
    tags$script(async = NA, src = "https://www.googletagmanager.com/gtag/js?id=G-9LR14C21Z6"),
    tags$script(HTML("
      window.dataLayer = window.dataLayer || [];
      function gtag(){dataLayer.push(arguments);}
      gtag('js', new Date());
      gtag('config', 'G-9LR14C21Z6');
      Shiny.addCustomMessageHandler('ga_tab_view', function(tab) {
        gtag('event', 'page_view', {page_title: tab, page_location: location.origin + '/#' + tab});
      });
    ")),
    tags$style(HTML(fbb_css)),
    tags$script(HTML(
      "function closeNavbarDropdowns() {
        var el = document.querySelector('.navbar-collapse');
        var colInst = el && bootstrap.Collapse.getInstance(el);
        if (colInst) colInst.hide();
        document.querySelectorAll('.navbar .dropdown-toggle')
          .forEach(function(toggle) {
            var inst = bootstrap.Dropdown.getInstance(toggle);
            if (inst) inst.hide();
            toggle.setAttribute('aria-expanded', 'false');
            var menu = toggle.nextElementSibling;
            if (menu) menu.classList.remove('show');
            toggle.parentElement && toggle.parentElement.classList.remove('show');
          });
      }
      Shiny.addCustomMessageHandler('collapse_navbar', function(x) {
        closeNavbarDropdowns();
        setTimeout(closeNavbarDropdowns, 50);
        setTimeout(closeNavbarDropdowns, 150);
        window.scrollTo({ top: 0, behavior: 'instant' });
      });
      // Team Importer slot visibility
      Shiny.addCustomMessageHandler('ti_slot_visibility', function(map) {
        Object.keys(map).forEach(function(id) {
          var el = document.getElementById(id);
          if (el) el.style.display = map[id] ? '' : 'none';
        });
      });
      // Keepalive: use a Web Worker for the timer so it isn't throttled when
      // the tab is backgrounded. Falls back to setInterval if Blob/Worker unavailable.
      (function() {
        function ping() {
          if (Shiny.shinyapp && Shiny.shinyapp.isConnected()) {
            Shiny.setInputValue('keepalive_ping', Date.now(), {priority: 'event'});
          }
        }
        try {
          var blob = new Blob(
            ['setInterval(function(){ postMessage(1); }, 30000);'],
            {type: 'application/javascript'}
          );
          var worker = new Worker(URL.createObjectURL(blob));
          worker.onmessage = function() { ping(); };
        } catch(e) {
          setInterval(ping, 30000);
        }
        // Also ping immediately when user returns to this tab
        document.addEventListener('visibilitychange', function() {
          if (!document.hidden) ping();
        });
      })();
      // Hash sync: update URL hash when Shiny navigates to a tab
      Shiny.addCustomMessageHandler('update_hash', function(val) {
        history.replaceState(null, null, val ? '#' + val : window.location.pathname);
      });
      // Hash sync: on connect, click the nav link matching the URL hash for deep-link nav
      $(document).on('shiny:connected', function() {
        var hash = window.location.hash.replace('#', '');
        if (!hash) return;
        function tryNav() {
          var link = document.querySelector('a[data-value=\"' + hash + '\"]');
          if (link) {
            link.click();
            setTimeout(closeNavbarDropdowns, 50);
            setTimeout(closeNavbarDropdowns, 150);
            return true;
          }
          return false;
        }
        if (!tryNav()) setTimeout(tryNav, 500);
      });"
    ))
  ),

  # ── Panels ──────────────────────────────────────────────────────────────────

  nav_panel(
    title = "Home",
    value = "home",
    homeUI("home")
  ),

  nav_menu(
    title = "Draft Tools",
    nav_panel(
      title = "Projection Aggregator",
      value = "proj_agg",
      projAggUI("proj_agg")
    ),
    nav_panel(
      title = "NFBC ADP Scraper",
      value = "adp",
      adpUI("adp")
    ),
    nav_panel(
      title = "Auction Value Calculator",
      value = "auction",
      aucValUI("auc_val")
    ),
    nav_panel(
      title = "Draft Lab",
      value = "draft_helper",
      draftLabUI("draft_lab")
    )
  ),

  nav_menu(
    title = "In-Season Tools",
    nav_panel(
      title = "RoS Projection Values",
      value = "inseason_lab",
      inseasonLabUI("inseason_lab")
    ),
    nav_panel(
      title = "Player Rater",
      value = "player_rater",
      playerRaterUI("player_rater")
    ),
    nav_panel(
      title = "Team Rater",
      value = "team_rater",
      traterUI("team_rater")
    ),
    nav_panel(
      title = "Park Factor Calendar",
      value = "park_calendar",
      parkCalendarUI("park_calendar")
    )
  ),

  nav_menu(
    title = "Leaderboards",
    nav_panel(
      title = "SP Skillz",
      value = "sp_skillz",
      spSkillzUI("sp_skillz")
    ),
    nav_panel(
      title = "RP Skillz",
      value = "rp_skillz",
      rpSkillzUI("rp_skillz")
    ),
    nav_panel(
      title = "Good Start Metric",
      value = "gsm",
      gsmUI("gsm")
    ),
    nav_panel(
      title = "Hitter Dashboard",
      value = "hit_dashboard",
      hitDashUI("hit_dashboard")
    ),
    nav_panel(
      title = "Pitcher Dashboard",
      value = "pit_dashboard",
      pitDashUI("pit_dashboard")
    ),
    nav_panel(
      title = "Park Factors",
      value = "park_factors",
      parkFactorsUI("park_factors")
    ),
    nav_panel(
      title = "Adjusted Barrels (aBrl)",
      value = "abrl_leaderboard",
      abrlLeaderboardUI("abrl_lb")
    )
  ),

  nav_menu(
    title = "Streamonators",
    nav_panel(
      title = "SP Streamonator",
      value = "sp_stream",
      spStreamUI("sp_stream")
    ),
    nav_panel(
      title = "Hitter Streamonator",
      value = "hit_stream",
      hitStreamUI("hit_stream")
    ),
    nav_panel(
      title = "SP Outlook",
      value = "sp_outlook",
      spOutlookUI("sp_outlook")
    ),
  ),

  nav_menu(
    title = "Methodology",
    nav_panel(
      title = "Streamonator: Weights & Thresholds",
      value = "methodology_streamonator_appendix",
      tags$iframe(
        src   = "methodology_streamonator_appendix.html",
        style = "width:100%; height:calc(100vh - 120px); border:none; display:block;",
        title = "Streamonator Weight & Threshold Validation"
      )
    ),
    nav_panel(
      title = "SP Skillz",
      value = "methodology_sp_skillz",
      tags$iframe(
        src   = "methodology_sp_skillz.html",
        style = "width:100%; height:calc(100vh - 120px); border:none; display:block;",
        title = "SP Skillz Methodology"
      )
    ),
    nav_panel(
      title = "Park Factors",
      value = "methodology_park_factors",
      tags$iframe(
        src   = "methodology_park_factors.html",
        style = "width:100%; height:calc(100vh - 120px); border:none; display:block;",
        title = "Park Factors Methodology"
      )
    ),
    nav_panel(
      title = "Team Rater",
      value = "methodology_team_rater",
      tags$iframe(
        src   = "methodology_team_rater.html",
        style = "width:100%; height:calc(100vh - 120px); border:none; display:block;",
        title = "Team Rater Methodology"
      )
    ),
    nav_panel(
      title = "Hitter Valuation",
      value = "methodology_hitter",
      tags$iframe(
        src   = "methodology_hitter_valuation.html",
        style = "width:100%; height:calc(100vh - 120px); border:none; display:block;",
        title = "Hitter Valuation Methodology"
      )
    )
  ),

  # ── Research ──────────────────────────────────────────────────────────────
  nav_menu(
    title = "Research",
    nav_panel(
      title = "HR–EV Relationship",
      value = "research_hr_ev",
      hrEvUI("hr_ev")
    ),
    nav_panel(
      title = "Bat Speed & EV",
      value = "research_bat_speed",
      batSpeedUI("bat_speed")
    ),
    nav_panel(
      title = "Pitcher K% Prediction",
      value = "research_csw",
      cswResearchUI("csw_research")
    ),
    nav_panel(
      title = "Hitter K% Prediction",
      value = "research_hitter_whiff",
      hitterWhiffUI("hitter_whiff_research")
    ),
    nav_panel(
      title = "Park HR/Barrel",
      value = "research_park_hr_barrel",
      parkHrBarrelUI("park_hr_barrel")
    ),
    nav_panel(
      title = "Adjusted Barrels (aBrl)",
      value = "research_adj_barrel",
      adjBarrelUI("adj_barrel")
    )
  )
)

# ── Server ────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {
  # Absorb keepalive pings from the JS heartbeat (no-op — just keeps the session alive)
  observeEvent(input$keepalive_ping, {}, ignoreInit = TRUE)

  # Hash sync: push URL hash whenever the active tab changes
  observeEvent(input$main_nav, {
    session$sendCustomMessage("update_hash", input$main_nav)
    session$sendCustomMessage("ga_tab_view", input$main_nav)
  }, ignoreInit = TRUE)


  homeServer("home", main_nav_id = "main_nav", root_session = session)
  hitDashServer("hit_dashboard")
  pitDashServer("pit_dashboard")
  inseasonLabServer("inseason_lab")
  playerRaterServer("player_rater")
  parkFactorsServer("park_factors")
  parkCalendarServer("park_calendar")
  gsmServer("gsm")
  hrEvServer("hr_ev")
  batSpeedServer("bat_speed")
  cswResearchServer("csw_research")
  hitterWhiffServer("hitter_whiff_research")
  parkHrBarrelServer("park_hr_barrel")
  adjBarrelServer("adj_barrel")
  abrlLeaderboardServer("abrl_lb")

  # Shared fetch triggers — incremented by Streamonator "Fetch Probables" to
  # populate SP Skillz and Team Rater modules when they haven't run yet.
  spz_fetch_trigger <- reactiveVal(0L)
  tr_fetch_trigger  <- reactiveVal(0L)

  spz_data <- spSkillzServer("sp_skillz", fetch_trigger = spz_fetch_trigger)
  rpSkillzServer("rp_skillz")
  projAggServer("proj_agg")
  aucValServer("auc_val")
  adpServer("adp")
  draftLabServer("draft_lab")
  tr_data  <- traterServer("team_rater", fetch_trigger = tr_fetch_trigger)
  sp_stream_data  <- spStreamServer("sp_stream",
                                    spz_data_ext      = spz_data,
                                    team_rater_data   = tr_data,
                                    spz_fetch_trigger = spz_fetch_trigger,
                                    tr_fetch_trigger  = tr_fetch_trigger)
  hit_stream_data <- hitStreamServer("hit_stream",
                                     spz_data_ext      = spz_data,
                                     team_rater_data   = tr_data,
                                     spz_fetch_trigger = spz_fetch_trigger,
                                     tr_fetch_trigger  = tr_fetch_trigger)
  spOutlookServer("sp_outlook",
                  spz_data_ext      = spz_data,
                  team_rater_data   = tr_data,
                  spz_fetch_trigger = spz_fetch_trigger,
                  tr_fetch_trigger  = tr_fetch_trigger)
  observeEvent(input$brand_click, {
    nav_select("main_nav", "home", session = session)
  }, ignoreInit = TRUE)
}

shinyApp(ui, server)
