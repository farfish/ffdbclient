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
ffdb_to_dlmtool_csv <- function (document_name, output = stdout(), instance = 'ffdb.farfish.eu') {
    null_to_na <- function (x) { ifelse(is.null(x), NA, ifelse(identical(x, "NA"), NA, x)) }

    doc <- ffdb_import('dlmtool', document_name, instance = instance)
    first_line <- TRUE
    write_line <- function(line_name, x) {
        cat(line_name, x, "\n", file = output, sep=",", append=!(first_line))
        first_line <<- FALSE
    }

    write_line('Name', as.character(doc$metadata[1, "species"]))
    write_line('Year', as.numeric(rownames(doc$catch)))
    for (i in seq_len(ncol(doc$catch))) {
        write_line(ifelse(i == 1, 'Catch', 'Abundance index'), as.numeric(doc$catch[,i]))
    }
    write_line('Duration t', length(rownames(doc$catch)))
    write_line('Average catch over time t', mean(doc$catch$catch))
    write_line('Depletion over time t', doc$constants[1, "depletion_over_time"])
    write_line('M', doc$constants[1, "M"])
    write_line('FMSY/M', as.numeric(null_to_na(doc$constants[1, "FMSY.M"])))
    write_line('BMSY/B0', as.numeric(null_to_na(doc$constants[1, "BMSY.B0"])))
    write_line('MSY', as.numeric(null_to_na(doc$constants[1, "MSY"])))
    write_line('BMSY', as.numeric(null_to_na(doc$constants[1, "BMSY"])))
    write_line('Length at 50% maturity', as.numeric(null_to_na(doc$constants[1, "length_at_50pc_maturity"])))
    write_line('Length at 95% maturity', as.numeric(null_to_na(doc$constants[1, "length_at_95pc_maturity"])))
    write_line('Length at first capture', as.numeric(null_to_na(doc$constants[1, "length_at_first_capture"])))
    write_line('Length at full selection', as.numeric(null_to_na(doc$constants[1, "length_at_full_selection"])))
    write_line('CAA', as.numeric(NA))  # TODO: What's the format of this?
    write_line('Current stock depletion', as.numeric(null_to_na(doc$constants[1, "current_stock_depletion"])))
    write_line('Current stock abundance', as.numeric(null_to_na(doc$constants[1, "current_stock_abundance"])))
    write_line('Von Bertalanffy K parameter', as.numeric(null_to_na(doc$constants[1, "Von_Bertalanffy_K"])))
    write_line('Von Bertalanffy Linf parameter', as.numeric(null_to_na(doc$constants[1, "Von_Bertalanffy_Linf"])))
    write_line('Von Bertalanffy t0 parameter', as.numeric(null_to_na(doc$constants[1, "Von_Bertalanffy_t0"])))
    write_line('Length-weight parameter a', as.numeric(null_to_na(doc$constants[1, "Length.weight_parameter_a"])))
    write_line('Length-weight parameter b', as.numeric(null_to_na(doc$constants[1, "Length.weight_parameter_b"])))
    write_line('Steepness', as.numeric(NA))
    write_line('Maximum age', as.numeric(null_to_na(doc$constants[1, "maximum_age"])))
    write_line('CV Catch', as.numeric(null_to_na(doc$cv[1, "catch"])))
    write_line('CV Depletion over time t', as.numeric(null_to_na(doc$cv[1, "depletion_over_time"])))
    write_line('CV Average catch over time t', as.numeric(null_to_na(doc$cv[1, "avg_catch_over_time"])))
    write_line('CV Abundance index', as.numeric(null_to_na(doc$cv[1, "abundance_index"])))
    write_line('CV M', as.numeric(null_to_na(doc$cv[1, "M"])))
    write_line('CV FMSY/M', as.numeric(null_to_na(doc$cv[1, "FMSY/M"])))
    write_line('CV BMSY/B0', as.numeric(null_to_na(doc$cv[1, "BMSY/B0"])))
    write_line('CV current stock depletion', as.numeric(null_to_na(doc$cv[1, "current_stock_depletion"])))
    write_line('CV current stock abundance', as.numeric(null_to_na(doc$cv[1, "current_stock_abundance"])))
    write_line('CV von B. K parameter', as.numeric(null_to_na(doc$cv[1, "Von_Bertalanffy_K"])))
    write_line('CV von B. Linf parameter', as.numeric(null_to_na(doc$cv[1, "Von_Bertalanffy_Linf"])))
    write_line('CV von B. t0 parameter', as.numeric(null_to_na(doc$cv[1, "Von_Bertalanffy_t0"])))
    write_line('CV Length at 50% maturity', as.numeric(null_to_na(doc$cv[1, "length_at_50pc_maturity"])))
    write_line('CV Length at first capture', as.numeric(null_to_na(doc$cv[1, "length_at_first_capture"])))
    write_line('CV Length at full selection', as.numeric(null_to_na(doc$cv[1, "length_at_full_selection"])))
    write_line('CV Length-weight parameter a', as.numeric(null_to_na(doc$cv[1, "Length-weight_parameter_a"])))
    write_line('CV Length-weight parameter b', as.numeric(null_to_na(doc$cv[1, "Length-weight_parameter_b"])))
    write_line('CV Steepness', as.numeric(NA))
    write_line('Sigma length composition', as.numeric(null_to_na(doc$cv[1, "length_composition"])))
    write_line('Units', 'metric tonnes')
    write_line('Reference OFL', as.numeric(NA))
    write_line('Reference OFL type', as.numeric(NA))
    write_line('CAL_bins', as.numeric(NA))
    write_line('MPrec', as.numeric(NA))
    write_line('LHYear', as.numeric(null_to_na(rownames(doc$catch)[[length(rownames(doc$catch))]])))
}

ffdb_to_dlmtool <- function (document_name, instance = 'ffdb.farfish.eu') {
    f <- tempfile(fileext = ".csv")
    ffdb_to_dlmtool_csv(document_name, output = f, instance = instance)
    d <- DLMtool::XL2Data(f)
    unlink(f)
    return(d)
}

# Example:
# ffdb_to_dlmtool('cape_verde_blue_shark_example')
