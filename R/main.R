#######################################################################
########################   HIGH FREQUECNY CHECKS  #####################
#######################################################################


# This script performs a list of checks on the survey data to ensure
# compliance with the data assurance plan
#
# This script is aimed at being run on a frequent basis during data
# collection and will generate a log file that can then be reviewed
# and completed by the field team.
# At the end of data collection, the cleaning logs that have been
# reviewed and completed by the field team will be "applied" to the
# raw survey data to generate the clean survey data

# LIST OF CHECKS
#
# Interview level
# 1. Check that all interviews have a uuid
# 2. Check that there are no duplicate observations
# 3. Check that date values fall within survey date range
# 4. Check that all surveys have consent
# 5. Check that interview duration is acceptable
# 6. Check that province match with sampling province
# 7. Check that certain critical variables are in credible range
# 8. Check consistency
# 9. Log "other, please specify" and text fields for translation
# and re-coding
#
# Enumerator level
# 1. Average interview time
# 2. Average percentage of "Don't know answers" per survey
# 3. Average rate of "Validated", "Cleaning" and "Deleted" surveys

# For ease of use, this script does not assume that the data it
# processes is only data that was never checked before. (i.e. the
# user can input the entire dataset every time the data is checked 
# during data collection). To avoid checking data twice and generating
# logs multiple times, the script uses a record of interviews that
# were checked in the past in the folder "logs". Therefore logs that
# have been generated in the past should be kept in this folder

library(readxl)
library(openxlsx)
library(dplyr)
library(tidyr)
library(tidyverse)
library(lubridate)
library(hash)
library(stringr)
source("utils.R")
source("qstandards_funcs.R")
source("tracker.R")

# get paths
ROOT <- "./.."

dir_list <- list.dirs(ROOT)

log_dir = file.path(ROOT, "logs")
data_dir = file.path(ROOT, "data")
qstandards_dir = file.path(ROOT, "quality_standards")
tool_dir = file.path(ROOT, "tool")
template_dir = file.path(ROOT, "template")
tracker_dir = file.path(ROOT, "trackers")

########### user parameters ###########
gen_log_file <- TRUE

# get path of the latest survey dataset
survey_data_path <- get_latest_csv(data_dir)
message("Performing checks on survey data based on file: ", basename(survey_data_path))

# get past logs
past_logs <- get_past_logs(log_dir)

# load survey data
survey_data <- read.csv(survey_data_path, head=TRUE,sep=";", stringsAsFactors = FALSE, na.strings = c("", "NA"))

# keep only survey data that was not previously checked
survey_data <- survey_data %>%
  rename(uuid = X_uuid) %>%
  anti_join(past_logs, by = "uuid")

# perform checks and generate logs if new data
if (nrow(survey_data > 0)) {
  
  message(sprintf("Found %s new surveys to check", nrow(survey_data)))

  # load the tool
  tool <- get_tool(tool_dir)
  
  # load data quality standards  
  quality_standards <- get_quality_standards(qstandards_dir)
  
  # load the sampling frame
  sf <- read_excel(file.path(ROOT, "phase2_sampling_frame.xlsx"))

  # get the quality standards logs
  qlogs <- quality_standards_checker(survey_data, quality_standards, sf, tool)
  
  # get translation logs
  translation_log <- translation_checker(survey_data, tool)
  
  # unwrap qstandards logs
  check_log <- qlogs[[1]]
  cleaning_log <- qlogs[[2]]

  # update tracker
  # get all logs from data collection
  all_logs <- bind_rows(past_logs, check_log)
  
  # get the clean data if any.
  # we get the clean data for tracking because the "sampling_value_chain" column was sometimes cleaned.
  clean_data_path <- get_latest_excel(file.path(data_dir, "clean_data"))
  if (!is_empty(clean_data_path)) {
    clean_data <- read_excel(clean_data_path)
    message(sprintf("Using %s and logs to generate tracker", clean_data_path))
  } else {
    clean_data <- NA
    message("Using logs only to generate tracker")
  }
  
  tracker <- get_tracker(all_logs, sf, clean_data)
  
  if (gen_log_file) {
    
    # generate the log file
    wb <- loadWorkbook(file.path(template_dir, "log_template.xlsx"))
    
    # write the check_log dataFrame to a separate sheet
    writeData(wb, sheet = "checked_interviews", check_log)
    
    # write the cleaning_log dataFrame to a separate sheet
    writeData(wb, sheet = "cleaning_logs", cleaning_log)
    
    # write the translation_log to a separate sheet
    writeData(wb, sheet = "translation_logs", translation_log)
    
    timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
    out_log_path <- file.path(log_dir, paste0("log_file_", timestamp, ".xlsx"))
    saveWorkbook(wb, file = out_log_path, overwrite = TRUE)
    message("Generated log file: ", out_log_path)
    
    # update the tracker sheet
    wb <- loadWorkbook(file.path(template_dir, "tracker_template.xlsx"))
    
    # write all logs to the logs sheet
    writeData(wb, sheet = "logs", all_logs)
    writeData(wb, sheet = "enumerator_perf", tracker$enum_perf)
    writeData(wb, sheet = "tracker", tracker$progress)
    
    out_tracker_path <- file.path(tracker_dir, paste0("tracker_", timestamp, ".xlsx"))
    saveWorkbook(wb, file = out_tracker_path, overwrite = TRUE)
    message("Updated tracker dashboard: ", out_tracker_path)

  }
  
} else {
  
  message("Found no new surveys to check")
  
}









