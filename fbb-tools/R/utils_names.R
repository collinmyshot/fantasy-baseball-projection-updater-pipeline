# ── utils_names.R ─────────────────────────────────────────────────────────────
# Shared player name normalization with override alias support.
# Sourced once by app.R before any module is loaded.
#
# Public API:
#   player_nk(x)   — normalize one or more names to a join key,
#                    folding diacritics and applying manual alias overrides
# ------------------------------------------------------------------------------

# Internal: raw normalization (no overrides)
.raw_nk <- function(x) {
  x <- iconv(as.character(x), from = "UTF-8", to = "ASCII//TRANSLIT")
  tolower(gsub("[^a-z0-9]", "", x))
}

# Override table: maps alt_nk → canonical_nk.
# Loaded from data/manual/player_match_overrides.csv at startup.
# Schema required: columns alt_name, canonical_name (plus optional notes).
# Add a row any time a player is known by different names across data sources
# (nicknames, shortened first names, middle-initial variants, etc.).
NAME_OVERRIDES <- local({
  path <- "data/manual/player_match_overrides.csv"
  tryCatch({
    df <- read.csv(path, stringsAsFactors = FALSE)
    if (nrow(df) == 0 || !all(c("alt_name", "canonical_name") %in% names(df)))
      return(character(0))
    alt_nk <- .raw_nk(df$alt_name)
    can_nk <- .raw_nk(df$canonical_name)
    keep   <- alt_nk != can_nk          # drop no-op rows
    if (!any(keep)) return(character(0))
    setNames(can_nk[keep], alt_nk[keep])
  }, error = function(e) character(0))
})

# Public: normalize player name(s) → join key, applying override aliases.
# Vectorized — safe to pass a whole column at once.
player_nk <- function(x) {
  nk <- .raw_nk(x)
  if (length(NAME_OVERRIDES) > 0) {
    hits     <- nk %in% names(NAME_OVERRIDES)
    nk[hits] <- NAME_OVERRIDES[nk[hits]]
  }
  nk
}
