get_latest_csv <- function(path) {
  
  # List all files with a .csv extension
  csv_files <- list.files(path = path, pattern = "\\.csv$")
  
  # Extract timestamps from filenames and convert to POSIXct datetime objects
  timestamps <- as.POSIXct(sub(".*?(\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}).*",
                               "\\1", csv_files), format = "%Y-%m-%d-%H-%M-%S")
  
  # Find the index of the latest file
  latest_index <- which.max(timestamps)
  
  # Get the filename of the latest CSV file
  latest_file <- csv_files[latest_index]
  
  # Return the full path to the latest CSV file
  return(file.path(path, latest_file))
}


get_latest_excel <- function(path) {
  
  # List all files with a .xlsx extension
  excel_files <- list.files(path = path, pattern = "\\.xlsx$")
  
  # Extract timestamps from filenames and convert to POSIXct datetime objects
  timestamps <- as.POSIXct(sub(".*?(\\d{8}-\\d{6}).*\\.xlsx", "\\1", excel_files), format = "%Y%m%d-%H%M%S")
  
  # Find the index of the latest file
  latest_index <- which.max(timestamps)
  
  # Get the filename of the latest CSV file
  latest_file <- excel_files[latest_index]
  
  # Return the full path to the latest CSV file
  return(file.path(path, latest_file))
}

get_past_logs <- function(log_dir) {
  
  # Function that gets previously generated cleaning logs if any
  
  # List all log files in the "logs" directory
  log_files <- list.files(path = log_dir, pattern ="\\.xlsx$", full.names = TRUE)
  
  # Check if there are any log files
  if (length(log_files) > 0) {
    
    # Initialize an empty list to store data from each file
    all_data <- list()
    
    # Loop through each log file
    for (file in log_files) {
      
      # Read the 'checked_interviews' sheet from each Excel file
      data <- read_excel(file, sheet = "checked_interviews")
      # Append the data to the list
      all_data[[file]] <- data
    }
    
    # Concatenate all data frames into one
    logs <- bind_rows(all_data)
    
  } else {
    
    template_path <- file.path("./../template/log_template.xlsx")
    
    # If no log files are found, open the 'template.xlsx' file
    if (file.exists(template_path)) {
      
      # Open the template file
      logs <- read_excel(template_path, sheet = "checked_interviews", col_types = "text")
      #logs$uuid <- toString(logs$uuid) 
      message("No log files found. Opening 'log_template.xlsx'.")
      
    } else {
      message("'log_template.xlsx' does not exist.")
    }
  }
  
  return(logs)
  
}

get_quality_standards <- function(check_dir) {
  
  file_path <- file.path(check_dir, "phase2_high_frequency_checks.xlsx")
  
  # List all sheet names in the Excel file
  sheet_names <- excel_sheets(file_path)
  
  # Read each sheet into a dataframe and store it in a list with names
  params <- setNames(
    lapply(sheet_names, function(sheet) {
      read_excel(file_path, sheet = sheet)
    }),
    sheet_names
  )
  
  return(params)
  
}

get_tool <- function(tool_dir) {
  
  tool = list()
  tool$survey <- read_excel(file.path(tool_dir, "tool_phase2_final.xlsx"), sheet = "survey")
  tool$choices <- read_excel(file.path(tool_dir, "tool_phase2_final.xlsx"), sheet = "choices")
  
  return(tool)
  
}




