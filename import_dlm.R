# Import from the FFDB database
# (c) 2018 Jamie Lentin - jamie.lentin@shuttlethread.com
library(httr)
library(jsonlite)
library(DLMtool)

# Fetch an FFDB document, convert it into a series of data frames
ffdb_import <- function (template_name, document_name, instance = 'ffdb.farfish.eu') {
    # Try as.numeric, if it generates warnings use as.character
    to_numeric_or_char <- function (l) {
        withCallingHandlers((function (m) {
            withRestarts(
                as.numeric(m),
                as_char_restart = as.character)
        })(l), warning = function (w) { 
            invokeRestart('as_char_restart', l)
        })
    }

    ffdb_to_data_frame <- function (json_df) {
        # Take a FFDB data.frame structure and convert it into R
        do.call(data.frame, c(list(
                row.names = json_df$`_headings`$values,
                stringsAsFactors = FALSE
            ), lapply(json_df[json_df$`_headings`$fields], to_numeric_or_char)))
    }

    req <- GET(paste0('https://', instance, '/api/doc/', template_name, '/', document_name),
        add_headers('Accept' = "application/json"))
    if (http_error(req)) {
        stop(sprintf("Request failed: status %s - URL '%s'", status_code(req), uri))
    }
    resp <- fromJSON(content(req, as = "text", encoding = "UTF-8"))
    lapply(resp$content, ffdb_to_data_frame)
}

# Fetch an FFDB "dlmtool" document, and convert it into a DLMtool Data object
ffdb_to_dlmtool <- function (document_name, instance = 'ffdb.farfish.eu') {
    doc <- ffdb_import('dlmtool', document_name, instance = instance)

    out <- new('Data')
    
    # metadata
    out@Name <- document_name
    out@Common_Name <- doc$metadata[1, "case_study"]
    out@Species <- doc$metadata[1, "species"]
    out@Region <- doc$metadata[1, "location"]

    # catch
    out@Year <- as.numeric(rownames(doc$catch))
    out@Cat <- matrix(doc$catch$catch, nrow=1, dimnames = list(c('Cat'), rownames(doc$catch)))
    out@Ind <- matrix(doc$catch$abundance_index, nrow=1, dimnames = list(c('Abun'), rownames(doc$catch)))
    out@t <- ncol(out@Cat)

    # constants
    out@AvC <- doc$constants[1, "avg_catch_over_time"]
    out@Dt <- doc$constants[1, "depletion_over_time"]
    out@Mort <- doc$constants[1, "M"]
    out@FMSY_M <- doc$constants[1, "FMSY.M"]
    out@BMSY_B0 <- doc$constants[1, "BMSY.B0"]
    out@L50 <- doc$constants[1, "length_at_50pc_maturity"]
    out@L95 <- doc$constants[1, "length_at_95pc_maturity"]
    out@LFC <- doc$constants[1, "length_at_first_capture"]
    out@LFS <- doc$constants[1, "length_at_full_selection"]

    # catch-at-age
    out@CAA <- array(
        do.call(c, doc$caa),
        dim = c(
            # nsim
            1,
            # nyears
            nrow(doc$caa),
            # MaxAge
            ncol(doc$caa)))

    out@Dep <- doc$constants[1, "current_stock_depletion"]
    out@Abun <- doc$constants[1, "current_stock_abundance"]
    out@vbK <- doc$constants[1, "Von_Bertalanffy_K"]
    out@vbLinf <- doc$constants[1, "Von_Bertalanffy_Linf"]
    out@vbt0 <- doc$constants[1, "Von_Bertalanffy_t0"]
    out@wla <- doc$constants[1, "Length-weight_parameter_a"]
    out@wlb <- doc$constants[1, "Length-weight_parameter_b"]

    # cv
    out@CV_Dt <- doc@cv[1, "depletion_over_time"]
    out@CV_AvC <- doc@cv[1, "avg_catch_over_time"]
    out@CV_Ind <- doc@cv[1, "abundance_index"]
    out@CV_Mort <- doc@cv[1, "M"]
    out@CV_FMSY_M <- doc@cv[1, "FMSY/M"]
    out@CV_BMSY_B0 <- doc@cv[1, "BMSY/B0"]
    out@CV_Dep <- doc@cv[1, "current_stock_depletion"]
    out@CV_Abun <- doc@cv[1, "current_stock_abundance"]
    out@CV_vbK <- doc@cv[1, "Von_Bertalanffy_K"]
    out@CV_vbLinf <- doc@cv[1, "Von_Bertalanffy_Linf"]
    out@CV_vbt0 <- doc@cv[1, "Von_Bertalanffy_t0"]
    out@CV_L50 <- doc@cv[1, "length_at_50pc_maturity"]
    out@CV_LFC <- doc@cv[1, "length_at_first_capture"]
    out@CV_LFS <- doc@cv[1, "length_at_full_selection"]
    out@CV_wla <- doc@cv[1, "Length-weight_parameter_a"]
    out@CV_wlb <- doc@cv[1, "Length-weight_parameter_b"]
    # TODO: No equivalent for "Imprecision in length composition data"?

    out@MaxAge <- doc$constants[1, "maximum_age"]

    # catch-at-length
    out@CAL_bins <- unlist(doc$cal[1,])  # The "Min Length" row
    out@CAL <- array(
        # Flatten everything apart from the first row of cal into a vector
        do.call(c, doc$cal[2:nrow(doc$cal),]),
        dim = c(
            # nsim
            1,
            # nyears
            nrow(doc$cal) - 1,
            # MaxAge
            ncol(doc$cal)))

    # TAC - The calculated catch limits (function TAC)
    # Sense - The results of the sensitivity analysis
    out@Units <- 'tonnes'

    out@Ref <- doc$constants[1, "ref_ofl_limit"]  # TODO: Not entirely convinced

    out@Cref <- doc$constants[1, "MSY"]
    out@Bref <- doc$constants[1, "BMSY"]

    return(out)
}

# Example:
ffdb_to_dlmtool('cape_verde_blue_shark_example')
