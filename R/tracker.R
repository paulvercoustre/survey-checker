#######################################################################
#####################  Data Collection Tracker  #######################
#######################################################################

# functions used to generate the tracker dataframes

library(tibble)

add_cols <- function(df, cols) {
  add <- cols[!cols %in% names(df)]
  if(length(add) != 0) df[add] <- NA
  return(df)
}

get_enum_perf <- function(logs) {
  
  # all_status <- unique(logs$status)
  cols <- c("validated" = 0, "deleted" = 0, "cleaning" = 0)
  # names(cols)
  
  # get the frequencies for survey status per enumerator
  survey_status <- logs %>%
    group_by(enumerator_id, status) %>%
    summarise(count = n(), .groups = "drop") %>%
    pivot_wider(names_from = status, values_from = count, values_fill = list(count = 0)) %>%
    add_column(!!!cols[!names(cols) %in% names(.)]) %>%
    mutate(Total = rowSums(select(., -enumerator_id))) %>%
    mutate(across(names(cols), ~ .x / Total * 100)) %>%
    select(-Total)
  
  # get the average interview duration and % of dk
  enum_perf <- logs %>%
    filter(!status == "deleted") %>%
    group_by(enumerator_id) %>%
    summarise(
      mean_survey_duration = mean(duration_minutes),
      mean_dk_rate = mean(percent_dk)) %>%
    left_join(survey_status, by = "enumerator_id")
  
  return(enum_perf)
  
}

get_tracker <- function(logs, sampling_frame, clean_data=NULL) {
  
  enum_perf <- get_enum_perf(logs)
  
  if (!is.null(clean_data)) {
    
    clean_logs <- clean_data %>%
      rename(uuid = X_uuid) %>%
      mutate(status = "validated") %>%
      select(uuid, enumerator_id, sampling_value_chain, sampling_province, status)
    
    logs_filtered <- anti_join(logs, clean_logs, by = "uuid") %>%
      select(uuid, enumerator_id, sampling_value_chain, sampling_province, status)
    
    logs <- bind_rows(clean_logs, logs_filtered)
    
  }
  
  # calculate progress aggregated per province and value chain
  progress_pvc <- logs %>%  
    filter(!status == "deleted") %>%
    group_by(sampling_province, sampling_value_chain) %>%
    summarise(interviews_conducted = n(), .groups = 'keep') %>%
    right_join(sampling_frame, by = c("sampling_province", "sampling_value_chain")) %>%
    mutate(progress_SME_listed = interviews_conducted/number_SME_listed) %>%
    mutate(progress_interview_target = interviews_conducted/interviews_target) %>%
    select(Region, Province, sampling_province, sampling_value_chain, interviews_target, number_SME_listed, interviews_conducted, progress_SME_listed, progress_interview_target) %>%
    arrange(Region, Province)
  
  tracker = list(progress = progress_pvc, enum_perf = enum_perf)
  
  return(tracker)
  
}