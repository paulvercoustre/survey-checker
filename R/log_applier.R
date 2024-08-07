#######################################################################
############################## LOG APPLIER  ###########################
#######################################################################

# this scripts applies the cleaning logs to the raw data and generates
# the clean_data


library(readxl)
library(openxlsx)
library(dplyr)
library(tidyr)
source("utils.R")

# get paths
ROOT <- "./.."

dir_list <- list.dirs(ROOT)
 
log_dir <- file.path(ROOT, "logs")
filled_log_dir = file.path(log_dir, "filled_logs")
data_dir = file.path(ROOT, "data")
qstandards_dir = file.path(ROOT, "quality_standards")
tool_dir = file.path(ROOT, "tool")
template_dir = file.path(ROOT, "template")
tracker_dir = file.path(ROOT, "trackers")


get_filled_logs <- function(filled_log_dir, log_type) {
  
  # Function that gets filled cleaning logs if any
  
  # List all log files in the "filled_logs" directory
  log_files <- list.files(path = filled_log_dir, pattern ="\\.xlsx$", full.names = TRUE)
  
  # Check if there are any log files
  if (length(log_files) > 0) {
    
    # Initialize an empty list to store data from each file
    all_data <- list()
    
    # Loop through each log file
    for (file in log_files) {
      
      # Read the 'checked_interviews' sheet from each Excel file
      data <- read_excel(file, sheet = log_type)
      
      # check whether there are logs
      if (nrow(data) > 0) {
  
        if ("new_value" %in% colnames(data)) {
          data$new_value <- as.character(data$new_value)  # make sure new_value has the correct type
        }
        
        # Append the data to the list
        all_data[[file]] <- data
        
      } else {
        message(sprintf("No %s in %s", log_type, file))
      }
    }
    
    # Concatenate all data frames into one
    logs <- bind_rows(all_data)
    
  } else {
    message("No filled logs found does not exist.")
    logs = NA
  }
  
  return(logs)
  
}


apply_logs <- function(survey_data, logs) {
  
  message("Number of logs generated: ", nrow(logs))
  filled_logs <- logs %>%
    filter(!is.na(new_value))
  
  message("Number of logs completed: ", nrow(filled_logs))
  
  for (i in 1:nrow(filled_logs)) {
    
    # Extract the necessary information for each row in the cleaning log
    uuid <- filled_logs$uuid[i]
    var_name <- filled_logs$variable_name[i]
    new_value <- filled_logs$new_value[i]
    
    # Determine the column type from survey_data
    column_type <- class(survey_data[[var_name]])
    
    # Convert new_value to the appropriate type
    if (column_type == "character") {
      new_value <- as.character(new_value)
    } else if (column_type == "integer") {
      new_value <- as.integer(new_value)
    } else if (column_type == "numeric") {
      new_value <- as.numeric(new_value)
    }
    
    # Update the survey_data
    survey_data <- survey_data %>%
      mutate(!!sym(var_name) := ifelse(uuid == .data$X_uuid, new_value, .data[[var_name]]))
  }
  
  clean_data <- survey_data
  
  return(clean_data)
  
}




# get path of the latest survey dataset
survey_data_path <- get_latest_csv(data_dir)
message("Applying logs on survey data based on file: ", basename(survey_data_path))

# load survey data
survey_data <- read.csv(survey_data_path, head=TRUE,sep=";", stringsAsFactors = FALSE, na.strings = c("", "NA"))
message("Number of surveys in raw data: ", nrow(survey_data))

# load the list of interviews with completed logs
completed_logs <- get_filled_logs(filled_log_dir, "checked_interviews") 
message("Number of surveys with completed cleaning logs: ", nrow(completed_logs))

deleted_surveys <- completed_logs %>%
  filter(status == 'deleted')

# remove rejected / deleted surveys
survey_data <- survey_data %>%
  filter(X_uuid %in% completed_logs$uuid) %>%
  filter(!X_uuid %in% deleted_surveys$uuid)
message("Number of surveys deleted: ", nrow(deleted_surveys))

# get all filled cleaning logs
cleaning_logs <- get_filled_logs(filled_log_dir, "cleaning_logs")
translation_logs <- get_filled_logs(filled_log_dir, "translation_logs")

translated_data <- apply_logs(survey_data, translation_logs)
clean_data <- apply_logs(translated_data, cleaning_logs)


### keep only columns needed for analysis
# load the kobo form
tool <- get_tool(tool_dir)

# Extract question types from the Kobo form
question_types <- tool$survey %>% select(type, name) %>% na.omit()

# get the list of questions names for select_one, select_multiple, integer and text from the survey sheet
meta_data_columns <- question_types %>% filter(grepl('start|end|today|calculate', type)) %>% pull(name)
integer_questions <- question_types %>% filter(grepl('integer', type)) %>% pull(name)
text_questions <- question_types %>% filter(grepl('text', type)) %>% pull(name)
select_one_questions <- question_types %>% filter(grepl('select_one', type)) %>% pull(name)

# select_multiple questions are one-hot encoded with columns named "question.answer_option"
# we get the answer option names from the kobo form to reconstruct column names
select_multiple_questions <- question_types %>% filter(grepl('select_multiple', type)) %>%
  separate(col = type, into = c("type", "list_name"), sep = " ", extra = "merge") %>%
  left_join(tool$choices, by = "list_name", relationship = "many-to-many") %>%
  mutate(
    name = paste(name.x, name.y, sep = ".")
  ) %>% pull(name)

# list columns to keep whilst keeping order
cols_to_keep <- intersect(names(clean_data), c(meta_data_columns, text_questions, integer_questions, select_one_questions, select_multiple_questions, "X_uuid"))
clean_data <- clean_data %>% select(all_of(cols_to_keep))


# write clean data to excel
timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
out_data_path <- file.path(data_dir, "clean_data", paste0("clean_data_", timestamp, ".xlsx"))
write.xlsx(clean_data, out_data_path)


