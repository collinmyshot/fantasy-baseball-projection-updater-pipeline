## Shared utilities for the fantasy baseball projection pipeline.
## Sourced by pipeline_config.R (and transitively by all scripts).
## Also sourced directly by park factor scripts via park_factors.R.

# --- Null coalescing operator ---
`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) y else x
}

# --- Comma-separated integer vector parsing ---
# Used by park factor and Statcast fetch scripts for --exclude-seasons, etc.
parse_int_vec <- function(x) {
  vals <- strsplit(x, ",", fixed = TRUE)[[1]]
  as.integer(trimws(vals))
}

# --- Strict numeric coercion for CLI args ---
# Returns fallback for empty strings; stops on non-numeric input.
parse_num <- function(x, fallback) {
  if (!nzchar(x)) return(fallback)
  val <- suppressWarnings(as.numeric(trimws(x)))
  if (length(val) != 1 || is.na(val)) {
    stop(sprintf("Invalid numeric value: %s", x))
  }
  val
}

# --- Replace NA with zero for scoring formulas ---
score_num <- function(x) ifelse(is.na(x), 0, x)

# --- CLI argument parsing ---
# Replaces the 20-160 line boilerplate blocks duplicated across scripts.
#
# spec: named list of argument definitions. Each element is a list with:
#   flag     — the CLI flag (e.g. "--config")
#   default  — default value if not supplied
#   type     — "string" (default), "boolean", or "numeric"
#   aliases  — character vector of alternative flags (optional)
#
# Returns a named list with parsed values plus $positional (character vector
# of leftover positional arguments).
#
# Example:
#   parsed <- parse_cli_args(list(
#     config = list(flag = "--config", default = "config/pipeline.yml"),
#     skip_sheets = list(flag = "--skip-sheets", type = "boolean", default = FALSE)
#   ))
#   parsed$config       # "config/pipeline.yml" or whatever was passed
#   parsed$positional   # any leftover positional args
parse_cli_args <- function(spec, args_raw = commandArgs(trailingOnly = TRUE),
                           allow_positional = TRUE) {
  result <- lapply(spec, function(s) s$default)
  positional <- character(0)

  # Build lookup: flag string -> list(name, type, is_alias)
  flag_lookup <- list()
  for (nm in names(spec)) {
    s <- spec[[nm]]
    typ <- s$type %||% "string"
    flags <- c(s$flag, s$aliases %||% character(0))
    for (f in flags) {
      flag_lookup[[f]] <- list(name = nm, type = typ)
    }
  }

  i <- 1L
  while (i <= length(args_raw)) {
    arg <- args_raw[[i]]

    # Check --flag=value style
    eq_pos <- regexpr("=", arg, fixed = TRUE)
    if (eq_pos > 0) {
      flag_part <- substring(arg, 1, eq_pos - 1)
      value_part <- substring(arg, eq_pos + 1)
      info <- flag_lookup[[flag_part]]
      if (!is.null(info)) {
        if (info$type == "boolean") {
          result[[info$name]] <- tolower(value_part) %in% c("true", "1", "yes")
        } else if (info$type == "numeric") {
          result[[info$name]] <- as.numeric(value_part)
        } else {
          result[[info$name]] <- value_part
        }
        i <- i + 1L
        next
      }
    }

    # Check --flag value style (or boolean --flag)
    info <- flag_lookup[[arg]]
    if (!is.null(info)) {
      if (info$type == "boolean") {
        result[[info$name]] <- TRUE
        i <- i + 1L
      } else {
        if (i >= length(args_raw)) {
          stop(sprintf("Flag '%s' requires a value.", arg))
        }
        val <- args_raw[[i + 1L]]
        if (info$type == "numeric") {
          result[[info$name]] <- as.numeric(val)
        } else {
          result[[info$name]] <- val
        }
        i <- i + 2L
      }
      next
    }

    # Positional or unknown
    if (allow_positional && !startsWith(arg, "--")) {
      positional <- c(positional, arg)
      i <- i + 1L
    } else {
      stop(sprintf("Unknown argument: %s", arg))
    }
  }

  result$positional <- positional
  result
}

# --- Data frame comparison (for Google Sheets skip-if-unchanged logic) ---
normalize_df_for_compare <- function(df) {
  out <- as.data.frame(df, stringsAsFactors = FALSE)
  for (nm in names(out)) {
    if (is.numeric(out[[nm]])) {
      out[[nm]] <- ifelse(is.na(out[[nm]]), NA, round(out[[nm]], 6))
    } else {
      out[[nm]] <- trimws(as.character(out[[nm]]))
      out[[nm]][out[[nm]] == ""] <- NA
    }
  }
  out
}

frames_equal <- function(a, b) {
  if (!identical(names(a), names(b))) return(FALSE)
  if (nrow(a) != nrow(b)) return(FALSE)
  aa <- normalize_df_for_compare(a)
  bb <- normalize_df_for_compare(b)
  identical(aa, bb)
}

# --- Standard index (100-centered, 10-unit SD scale) ---
std_index <- function(x) {
  x <- as.numeric(x)
  m <- mean(x, na.rm = TRUE)
  s <- stats::sd(x, na.rm = TRUE)
  if (!is.finite(s) || s == 0) {
    return(rep(100, length(x)))
  }
  100 + 10 * ((x - m) / s)
}

# --- Safe numeric coercion for Google Sheets list columns ---
# googlesheets4 returns list columns when cells have mixed types, are empty,
# or contain hyperlinks (returned as named lists like list(href=..., label=...)).
# unlist() on nested lists produces wrong-length vectors; vapply guarantees
# exactly one numeric value per input row.
flatten_to_numeric <- function(x) {
  if (!is.list(x)) return(suppressWarnings(as.numeric(x)))
  vapply(x, function(v) {
    if (is.null(v) || length(v) == 0) return(NA_real_)
    suppressWarnings(as.numeric(v[[1]]))
  }, numeric(1))
}

# --- Safe CSV reader ---
safe_read_csv <- function(path, ...) {
  if (!nzchar(path) || !file.exists(path)) return(NULL)
  tryCatch(
    utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE, ...),
    error = function(e) NULL
  )
}
