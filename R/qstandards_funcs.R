#######################################################################
#####################  Quality standards functions  ###################
#######################################################################

# set of functions that implement the data quality assurance plan

is_uuid <- function(x) {
  
  # Function to check if a single value is a valid UUID
  grepl("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", x)
}

check_duplicate_uuids <- function(data, column_name) {
  
  # Function to check for duplicate UUIDs in a column
  
  # Extract the column by name
  uuid_column <- data[[column_name]]
  
  # Find all duplicated entries, including the first occurrence
  duplicates <- uuid_column[duplicated(uuid_column) | duplicated(uuid_column, fromLast = TRUE)]
  
  # Return the duplicated UUIDs
  unique(duplicates)  # Use `unique` to list each duplicated UUID only once
}

kobo_time_parser_UTC <- function(instring) {
  
  # Remove six digits of microseconds and colons, and replace 'T' with a space
  tmp <- gsub("\\.\\d{3}|:", "", instring)  # Removes microseconds and colons
  tmp <- gsub("T", " ", tmp)
  tmp <- gsub(" ", "", tmp)                 # Remove space between date and time for parsing
  
  # Format the string to fill with zeros if needed (not typically required here)
  tmp <- format(tmp, justify = "left", width = 22)
  
  # Parse the datetime string into a POSIXct object, considering the timezone
  as.POSIXct(strptime(tmp, format = "%Y-%m-%d%H%M%S%z", tz = "UTC"))
  
}

#' Perform Basic Data Integrity Checks
#'
#' This function runs a series of basic data integrity checks on survey data,
#' including checking for invalid UUIDs, duplicate UUIDs, out-of-date interviews, and interview durations.
#' Issues found are logged, and entries failing checks are removed from the dataset.
#'
#' @param data A dataframe containing survey data, expected to include fields such as UUIDs,
#'        enumerator IDs, sampling values, and provinces, along with interview timings and dates.
#' @param check_log A dataframe intended to log details of data entries that were removed based on the checks performed.
#'        This log is updated with entries for each check conducted.
#' @param quality_standards A list or data structure containing the quality control parameters such as 
#'        the start and end dates for data collection, and the acceptable minimum and maximum interview durations.
#'        This parameter is crucial for defining the rules based on which the data integrity checks are performed.
#'
#' @return A list containing two dataframes:
#'         - `data`: the cleaned dataframe after removing entries failing the integrity checks.
#'         - `check_log`: the updated log dataframe with details of the checks and deletions.
#'
#' @examples
#' data <- data.frame(
#'   uuid = c("uuid1", "uuid2", "uuid3", "uuid4"),
#'   enumerator_id = c(1, 2, 3, 4),
#'   sampling_value_chain = c("value1", "value2", "value3", "value4"),
#'   sampling_province = c("province1", "province2", "province3", "province4"),
#'   date = c("01/04/2024", "02/04/2024", "01/04/2024", "04/04/2024"),
#'   start = c("2024-04-01 09:00:00", "2024-04-02 10:00:00", "2024-04-01 09:30:00", "2024-04-04 11:00:00"),
#'   end = c("2024-04-01 10:00:00", "2024-04-02 11:15:00", "2024-04-01 09:45:00", "2024-04-04 11:05:00")
#' )
#' check_log <- data.frame()
#' quality_standards <- list(quality_params = list(start_date = as.Date("2024-04-01"),
#'                                                 end_date = as.Date("2024-04-30"),
#'                                                 min_duration_delete = 10,
#'                                                 max_duration_delete = 120))
#'
#' result <- basic_checks(data, check_log, quality_standards)
#' cleaned_data <- result$data
#' updated_log <- result$check_log
#'
#' @import dplyr
#' @export
basic_checks <- function(data, check_log, quality_standards) {
  # Ensure dependencies are loaded
  require(dplyr)
  require(lubridate)
  
  quality_params <- quality_standards$quality_params  # Assume quality_standards is defined globally
  
  data$date <- as.Date(data$date, format = "%Y-%m-%d")
  data$start <- kobo_time_parser_UTC(data$start)
  data$end <- kobo_time_parser_UTC(data$end)
  data$duration_minutes <- as.numeric(difftime(data$end, data$start, units = "mins"))
  
  checks <- data %>%
    mutate(invalid_uuid = !is_uuid(uuid)) %>% # Invalid UUID check and logging
    mutate(duplicate_uuid = duplicated(uuid) | duplicated(uuid, fromLast = TRUE)) %>% # # Duplicate UUID check and logging
    mutate(no_consent = consent == 'no') %>% # No consent check and logging
    mutate(out_of_dc_period = !between(date, quality_params$start_date, quality_params$end_date)) %>%
    mutate(interview_duration = duration_minutes < quality_params$min_duration_delete | duration_minutes > quality_params$max_duration_delete) %>%
    select(uuid, invalid_uuid, duplicate_uuid, no_consent, out_of_dc_period, interview_duration)
  
  check_log <- checks %>%
    pivot_longer(cols = -uuid, names_to = "condition", values_to = "value") %>% #  Reshape the data from wide to long format
    filter(value) %>% #  Filter rows where the condition is TRUE
    group_by(uuid) %>%
    summarise(reasons = toString(condition)) %>% # Summarize to list the condition names
    mutate(status = "deleted") %>%
    left_join(., data, by="uuid") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, status, reasons, duration_minutes)
  
  data <- data %>%
    filter(!uuid %in% check_log$uuid)
  
  return(list(data = data, check_log = check_log))
}

value_range_checks <- function(data, value_checks, cleaning_log) {
  
  # Function that runs the value range checks as defined by user
  # in phase2_high_frequency_checks.xlsx, tab "value_checks"
  
  # check value ranges
  # we iterate over the list of checks defined in value_checks df
  for(i in 1:nrow(value_checks)) {
    row <- value_checks[i,]
    
    if (row$issue_type == 'high_value') {
      value_cleaning_log <- data %>%
        filter(!!sym(row$variable_name) > row$threshold_value) %>%
        mutate(variable_name = row$variable_name, issue = row$issue_type, "old_value" = !!sym(row$variable_name), "new_value" = "") %>%
        select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
      
    } else if (row$issue_type == 'low_value') {
      value_cleaning_log <- data %>%
        filter(!!sym(row$variable_name) < row$threshold_value & !!sym(row$variable_name) != -1) %>%
        mutate(variable_name = row$variable_name, issue = row$issue_type, "old_value" = !!sym(row$variable_name), "new_value" = "") %>%
        select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
    }
    
    # make the column "old_value" as character for compatibility with other entries
    value_cleaning_log$old_value = as.character(value_cleaning_log$old_value)
    
    # log to the cleaning_log df
    cleaning_log <- bind_rows(cleaning_log, value_cleaning_log)
  }
  
  return(cleaning_log)
  
}

consistency_checks <- function(data, sf, cleaning_log) {
  
  # Function to urn the consistency checks on the data
  # These are manually defined as per the file phase2_high_frequency_checks.xlsx, tab "consistency_checks"
  sf_cleaning_log <- anti_join(data, sf, by = c("sampling_province", "sampling_value_chain")) %>%
    mutate(variable_name = "sampling_value_chain", issue = "value_chain_not_in_sampling_frame", "old_value" = sampling_value_chain, "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  # log to the cleaning_log df
  cleaning_log <- bind_rows(cleaning_log, sf_cleaning_log)
  
  # check if province sampling frame does not match with reported province
  cleaning_frame_mismatch <- data %>%
    filter(!sampling_province == province) %>%
    mutate(variable_name = "province", issue = "sampling_frame_mismatch", "old_value" = province, "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  # log province sampling frame mismatch
  cleaning_log <- bind_rows(cleaning_log, cleaning_frame_mismatch)
  
  # check and log for consistency in answers 
  check_1 <- data %>%
    filter(no_exports_reasons.no_international_quality_certifications == 1 & certification_status == "yes") %>%
    mutate(variable_name = "no_exports_reasons.no_international_quality_certifications", issue = "inconsistent_data", "old_value" = "1", "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_1)
  
  check_2 <- data %>%
    filter(sales_type == "indirect_exports" & sales_channels == "directly_to_customers") %>%
    mutate(variable_name = "sales_channels.directly_to_customers", issue = "inconsistent_data", "old_value" = "1", "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_2)
  
  check_3 <- data %>%
    filter(main_market == "international_market" & sales_type == "national_sales") %>%
    mutate(variable_name = "main_market", issue = "inconsistent_data", "old_value" = main_market, "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_3)
  
  check_4 <- data %>% 
    filter(competitor_number > 0 & competition_strategy == "no_products_competing") %>%
    mutate(variable_name = "competition_strategy", issue = "inconsistent_data", "old_value" = competition_strategy, "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_4)
  
  check_5 <- data %>%
    filter(competitor_number == 0 & innovation_contraint.market_dominated_established_players == 1) %>%
    mutate(variable_name = "innovation_contraint", issue = "inconsistent_data", "old_value" = "market_dominated_established_players", "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_5)
  
  check_6 <- data %>%
    filter(growth_opportunities == "no_opportunities" & investment_plan == "yes") %>%
    mutate(variable_name = "growth_opportunities", issue = "inconsistent_data", "old_value" = growth_opportunities, "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_6)
  
  check_7 <- data %>%
    filter(!is.null(year_last_loan) & as.Date(paste(year_last_loan, "-01", sep=""), format="%Y-%m-%d") < as.Date(paste(creation_date, "-01", sep=""), format="%Y-%m-%d")) %>%
    mutate(variable_name = "year_last_loan", issue = "inconsistent_data", "old_value" = toString(year_last_loan), "new_value" = "") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_7)
  
  check_8 <- data %>%
    filter(received_loan == "no" & (fund_source_operations.borrowed_bank == 1 | fund_source_operations.borrowed_microfinance == 1 
                                    | fund_source_assets.borrowed_microfinance == 1 | fund_source_assets.borrowed_microfinance == 1)) %>%
    mutate(variable_name = "received_loan", issue = "inconsistent_data", "old_value" = received_loan, "new_value" = "yes") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_8)
  
  check_9 <- data %>%
    filter(business_environment_contraint.inadequately_skilled_workforce == 1 & lack_individuals_obstacle == "not_an_obstacle") %>%
    mutate(variable_name = "business_environment_contraint.inadequately_skilled_workforce", issue = "inconsistent_data", "old_value" = "1", "new_value" = "0") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_9)
  
  check_10 <- data %>%
    filter(business_environment_contraint.access_to_finance == 1 & lack_finance_obstacle == "not_an_obstacle") %>%
    mutate(variable_name = "business_environment_contraint.access_to_finance", issue = "inconsistent_data", "old_value" = "1", "new_value" = "0") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, variable_name, issue, old_value, new_value)
  
  cleaning_log <- bind_rows(cleaning_log, check_10)
  
  return(cleaning_log)
  
}

dk_rate_check <- function(data, tool) {
  
  # Function that checks the number and percentage of "Don't know/Don't want to answer" entries in the dataset
  # We assume these entries are marked as "dk" (select one or multiple quetions), -1 or space only strings
  
  # get the list of answer options that include a "dk" answer option
  dk_answer_options <- tool$choices %>%
    filter(name == "dk") %>%
    select(list_name, name)
  
  # get the list of questions with a dk answer option, text or integer
  pattern <- paste(unlist(dk_answer_options$list_name), collapse = "|")
  dk_questions <- tool$survey %>%
    mutate(contains_dk = grepl(pattern, type, ignore.case = TRUE)) %>%
    filter(type %in% c("text", "integer") | contains_dk == TRUE) %>%
    select(name)
  
  log <- data %>%
    mutate(across(where(is.character), ~str_replace_all(., " ", ""))) %>%
    mutate(across(everything(), as.character)) %>%  # Convert all columns to character
    rowwise() %>%
    mutate(count_dk = sum(c_across(everything()) %in% c("dk", "-1"), na.rm = TRUE)) %>%
    mutate(percent_dk = count_dk / nrow(dk_questions)) %>%
    select(uuid, count_dk, percent_dk)
  
  return(log)
  
} 

quality_standards_checker <- function(data, quality_standards, sf, tool) {
  
  # Function that runs all the quality standards checks on the data as
  # set by user in quality_standards input file
  #
  # This includes consistency checks, value checks, and other logical checks
  # This generates the check_log and cleaning_log
  
  # create the log dataframes
  check_log <- data.frame()
  cleaning_log <- data.frame()
  
  # run basic quality checks
  dfs <- basic_checks(data, check_log, quality_standards)
  data <- dfs$data
  check_log <- dfs$check_log
  
  # run the value range checks
  cleaning_log <- value_range_checks(data, quality_standards$value_checks, cleaning_log)
  
  # run the consistency checks
  cleaning_log <- consistency_checks(data, sf, cleaning_log)
  
  # run "Don't know" prevalence check
  dk_log <- dk_rate_check(data, tool)
  
  # add the list of interviews with cleaning logs to the check_logs
  cleaning_data <- data %>% filter(uuid %in% cleaning_log$uuid) %>%
    mutate(status = "cleaning") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, status, duration_minutes)
  
  # get the list of validated surveys
  validated_data <- data %>%
    filter(!uuid %in% check_log$uuid & !uuid %in% cleaning_log$uuid) %>%
    mutate(status = "validated") %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, status, duration_minutes) 
  
  # add to logs
  check_log <- bind_rows(check_log, cleaning_data)
  check_log <- bind_rows(check_log, validated_data)
  check_log <- left_join(check_log, dk_log, by="uuid")
  
  # harmonize enumerator_id capitalization
  check_log$enumerator_id <- toupper(check_log$enumerator_id)
  cleaning_log$enumerator_id <- toupper(cleaning_log$enumerator_id)
  
  # get the province name
  province_list <- tool$choices %>%
    filter(list_name == "province_list") %>%
    select(name, `label::English`)
  
  check_log <- left_join(check_log, province_list, by = c('sampling_province'='name')) %>%
    rename(province_name = `label::English`) %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, province_name, status, reasons, duration_minutes, count_dk, percent_dk)
  
  cleaning_log <- left_join(cleaning_log, province_list, by = c('sampling_province'='name')) %>%
    rename(province_name = `label::English`) %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, province_name, variable_name, issue, old_value, new_value)
  
  # sort the cleaning logs for user readability
  cleaning_log <- cleaning_log[order(cleaning_log$uuid),]
  logs <- list(check_log, cleaning_log)
  
  return(logs)
  
}

translation_checker <- function(data, tool) {
  
  # Function that generates translation logs
  
  # list of text columns that should not generate translation logs
  ignore_cols <- c("enumerator_id", "creation_date", "year_last_loan")
  
  # get the list of all text columns in the tool
  text_cols <- tool$survey %>%
    filter(type == "text" & !name %in% ignore_cols) %>%
    select(name)

  # list of base columns that should appear in all logs
  base_cols <- c("uuid", "enumerator_id", "sampling_value_chain", "sampling_province")
  cols <- c(base_cols, pull(text_cols))
  
  # create the translation logs
  translation_logs <- data %>%
    select(all_of(cols)) %>%
    mutate(across(everything(), as.character)) %>%
    pivot_longer(
      cols = pull(text_cols), 
      names_to = "variable_name", 
      values_to = "old_value"
      ) %>%
    filter(!is.na(old_value)) %>%
    mutate(new_value = "", issue = "translation") %>%
    select(all_of(base_cols), variable_name, issue, old_value, new_value)
  
  # get the province name
  province_list <- tool$choices %>%
    filter(list_name == "province_list") %>%
    select(name, `label::English`)
  
  translation_logs <- left_join(translation_logs, province_list, by = c('sampling_province'='name')) %>%
    rename(province_name = `label::English`) %>%
    select(uuid, enumerator_id, sampling_value_chain, sampling_province, province_name, variable_name, issue, old_value, new_value)

  return(translation_logs)
  
}

