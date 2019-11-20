
# LIBRARIES ---------------------------------------------------------------

library(data.table)
library(h2o)
library(tidyverse, warn.conflicts = FALSE)
library(multidplyr)
library(lubridate)
library(anytime)
library(h3)
library(geosphere)
library(stringr)

# CLUSTER INITIATION ------------------------------------------------------

cluster <- new_cluster(12) %>%
  cluster_library(c('h3', 'geosphere', 'lubridate', 'anytime', 'tidyr', 'dplyr'))


# GLOBAL VARIABLES --------------------------------------------------------

FILE_PATH <- '/home/jenkins/mobileData/mobile_20191116'

# READ DATA (FUNCTION) ----------------------------------------------------

read_grouped_data_time_distance_speed <- function(x) {
  y <- fread(x, select = c(1, 7, 8, 4), quote = "") %>%
    
    set_names(c('id',
                'lat',
                'lon',
                'timestamp')) %>%
    
    filter(lat != 0,
           lon != 0,
           is.na(lat) == FALSE,
           is.na(lon) == FALSE) %>%
    
    # convert microseconds to miliseconds
    # otherwise partition operation will break it
    mutate(
      timestamp = timestamp / 1000,
      htime = round_date(anytime(timestamp), unit = 'hour'),
      hour = as.integer(strftime(htime, "%H")),
      
      index_h3 = geo_to_h3(c(lat, lon), 12)
    ) %>%
    
    # 1st - initiate parallel calculations
    # assigning next timestamps/coordinates
    group_by(id) %>%
    partition(cluster = cluster) %>%
    
    # sort by timestamp to perform lead() opearations
    # assigning next bid timestamp
    arrange(timestamp) %>%
    
    mutate(
      # Time
      next_timestamp = lead(timestamp),
      
      # Distance
      lat_next = lead(lat),
      lon_next = lead(lon),
      distance = distVincentyEllipsoid(cbind(lon, lat), cbind(lon_next, lat_next))
    ) %>%
    
    collect() %>%
    
    # 2nd - initiate parallel calculations
    # aggregating distance and full time per hour (htime)
    group_by(id, htime) %>%
    partition(cluster = cluster) %>%
    
    summarise(
      distance = sum(distance, na.rm = TRUE)/1000,
      full_time = (max(timestamp) - min(timestamp))/3600,
      avg_speed = distance / full_time,
      index_h3 = n_distinct(index_h3)
    ) %>%
    
    collect()
  
  return(y)
}

read_grouped_data_country_device_timestamp <- function(x) {
  y <- fread(x, select = c(1, 7, 8, 10, 5, 4), quote = "") %>%
    
    set_names(c('id',
                'lat',
                'lon',
                'country',
                'device',
                'timestamp')) %>%
    
    mutate(timestamp = timestamp / 1000,
           htime = round_date(anytime(timestamp), unit = 'hour')) %>%
    
    group_by(id, htime) %>%
    
    partition(cluster = cluster) %>%
    
    mutate(device = tolower(device),
           hour = as.integer(strftime(htime, "%H"))) %>%
    
    summarise(
      tot_contacts = n(),
      countries = n_distinct(country[!is.na(country)]),
      devices = n_distinct(device[!is.na(device)])
    ) %>%
    
    collect()
  
  return(y)
}

# LOAD DATA ---------------------------------------------------------------

time_distance_speed_h3 <- read_grouped_data_time_distance_speed(FILE_PATH)
country_device_contacts <- read_grouped_data_country_device_timestamp(FILE_PATH)

# MERGING DATA ------------------------------------------------------------

full_data <- full_join(time_distance_speed_h3, country_device_contacts, by = c('id', 'htime')) %>% 
  
  mutate(hour = as.integer(strftime(htime, "%H"))) 

# DATA MUNGING ------------------------------------------------------------

id_to_remove <- full_data %>% 
  
  # Users whose average speed was infinity or equal or more than 250 km/h
  # Users who had equal or more thank 60 bids during any hour
  # Users who used at least 4 different devices
  filter(avg_speed == 'Inf' | avg_speed >= 250 |
         tot_contacts >= 60 |
         devices >= 4
         ) %>% 
  
  # Selecting id's 
  # Filtering unique values 
  # Parsing to vector format
  select(id) %>% 
  unique() %>% 
  pull()


# real_data <- full_data[!full_data$id %in% id_to_remove,]
# bot_data <- full_data[full_data$id %in% id_to_remove,]

# Separate data used in isolation forest algorithm
isolation_data <- full_data[!full_data$id %in% id_to_remove, 
                            c('id','hour','avg_speed','index_h3','tot_contacts','countries','devices')] %>% 

  mutate(avg_speed = ifelse(is.na(avg_speed) == TRUE, 0, avg_speed),
         index_h3 = ifelse(is.na(index_h3) == TRUE, 1, index_h3))


# ISOLATION FOREST --------------------------------------------------------

# Initiate H2O
h2o.init()

# Reformat data to h2o format
isolation_data_h2o <- as.h2o(isolation_data)

# Train model
trainingModel <- h2o.isolationForest(training_frame = isolation_data_h2o[,3:7],
                                     sample_rate = 0.15, 
                                     max_depth = 15,
                                     ntrees = 100)

# Calculate score for all data
score = h2o.predict(trainingModel, isolation_data_h2o[,3:7])
result_pred = as.vector(score$predict)

# Setting desired threshold percentage
threshold = .995

# Using this threshold to get score limit to filter data anomalies.
scoreLimit = round(quantile(result_pred, threshold), 4) 

# Add row score at the beginning of dataset
# isolation_data <- isolation_data[,2:8]
isolation_data <- cbind(RowScore = round(result_pred, 4), isolation_data)

# Get data anomalies by filtering all data
# anomalies <- anomalies[,2:8]
anomalies = isolation_data[isolation_data$RowScore > scoreLimit,]

# HUMAN DATA --------------------------------------------------------------

id_to_remove_anomalies <- anomalies %>% 
  select(id) %>% 
  unique() %>% 
  pull()

human_data <- isolation_data[!isolation_data$id %in% id_to_remove_anomalies,] %>% 
  select(-RowScore)

# BOT DATA ----------------------------------------------------------------

bot_id <- c(id_to_remove, id_to_remove_anomalies) %>% unique()

bot_data <- full_data[full_data$id %in% bot_id,] %>% 
  select(id, hour, distance, avg_speed, index_h3, tot_contacts, countries, devices)
