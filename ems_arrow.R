# NYC EMS Incidents Dispatch Data 

library(arrow)
library(dplyr)
library(tictoc)

### Convert single large file to parquet files

list.files("data")
# [1] "EMS_Incident_Dispatch_Data_20250119.csv"

list.files("data", full.names = T) |> 
  file.size() |> 
  scales::comma()
# 6,317,252,621 Bytes
# 5.88 GB

tic()
ems_nyc_full <- open_dataset(
  sources = "data/EMS_Incident_Dispatch_Data_20250119.csv", 
  col_types = schema(),
  format = "csv"
)
toc() # 0.07 sec

tic()
glimpse(ems_nyc_full)
toc() # 40.22 sec elapsed

# FileSystemDataset with 1 csv file
# 27,223,682 rows x 31 columns
# $ CAD_INCIDENT_ID                 <int64> 71262688, 71262787, …
# $ INCIDENT_DATETIME              <string> "05/06/2007 06:21:01…
# $ INITIAL_CALL_TYPE              <string> "STNDBY", "UNC", "CA…
# $ INITIAL_SEVERITY_LEVEL_CODE     <int64> 8, 2, 3, 5, 7, 2, 6,…
# $ FINAL_CALL_TYPE                <string> "STNDBY", "UNC", "CA…
# $ FINAL_SEVERITY_LEVEL_CODE       <int64> 8, 2, 3, 5, 7, 2, 6,…
# $ FIRST_ASSIGNMENT_DATETIME      <string> "", "05/06/2007 06:5…
# $ VALID_DISPATCH_RSPNS_TIME_INDC <string> "N", "Y", "Y", "Y", …
# $ DISPATCH_RESPONSE_SECONDS_QY    <int64> 0, 12, 60, 0, 154, 5…
# $ FIRST_ACTIVATION_DATETIME      <string> "", "05/06/2007 06:5…
# $ FIRST_ON_SCENE_DATETIME        <string> "", "05/06/2007 07:0…
# $ VALID_INCIDENT_RSPNS_TIME_INDC <string> "N", "Y", "N", "Y", …
# $ INCIDENT_RESPONSE_SECONDS_QY    <int64> NA, 391, NA, 0, 262,…
# $ INCIDENT_TRAVEL_TM_SECONDS_QY   <int64> NA, 379, NA, 0, 108,…
# $ FIRST_TO_HOSP_DATETIME         <string> "", "", "", "05/06/2…
# $ FIRST_HOSP_ARRIVAL_DATETIME    <string> "", "", "", "05/06/2…
# $ INCIDENT_CLOSE_DATETIME        <string> "05/06/2007 06:21:01…
# $ HELD_INDICATOR                 <string> "N", "N", "N", "N", …
# $ INCIDENT_DISPOSITION_CODE      <string> "NOTSNT", "90", "87"…
# $ BOROUGH                        <string> "QUEENS", "BRONX", "…
# $ INCIDENT_DISPATCH_AREA         <string> "Q2", "B3", "Q2", "M…
# $ ZIPCODE                         <int64> NA, NA, NA, 10036, 1…
# $ POLICEPRECINCT                  <int64> NA, NA, NA, 14, 47, …
# $ CITYCOUNCILDISTRICT             <int64> NA, NA, NA, 3, 12, 1…
# $ COMMUNITYDISTRICT               <int64> NA, NA, NA, 104, 212…
# $ COMMUNITYSCHOOLDISTRICT         <int64> NA, NA, NA, 2, 11, 2…
# $ CONGRESSIONALDISTRICT           <int64> NA, NA, NA, 10, 16, …
# $ REOPEN_INDICATOR               <string> "N", "N", "N", "N", …
# $ SPECIAL_EVENT_INDICATOR        <string> "N", "N", "N", "N", …
# $ STANDBY_INDICATOR              <string> "Y", "N", "N", "N", …
# $ TRANSFER_INDICATOR             <string> "N", "N", "N", "N", …


# identify an appropriate variable to group by
tic()
ems_nyc_full |> 
  group_by(BOROUGH) |> 
  count(sort = T) |> 
  collect()
toc()
# 13.48 sec elapsed

# A tibble: 6 × 2
# Groups:   BOROUGH [6]
# BOROUGH                        n
# <chr>                      <int>
# 1 BROOKLYN                 7747447
# 2 MANHATTAN                6717310
# 3 BRONX                    6316780
# 4 QUEENS                   5314633
# 5 RICHMOND / STATEN ISLAND 1127300
# 6 UNKNOWN                      212


tic()
ems_nyc_full |>
  group_by(BOROUGH) |> 
  write_dataset(path = "data/ems-nyc", 
                format = "parquet")
toc()
# 55.36 sec elapsed

# resulting folders containing data
list.files("data/ems-nyc")
# [1] "BOROUGH=BRONX"                           
# [2] "BOROUGH=BROOKLYN"                        
# [3] "BOROUGH=MANHATTAN"                       
# [4] "BOROUGH=QUEENS"                          
# [5] "BOROUGH=RICHMOND%20%2F%20STATEN%20ISLAND"
# [6] "BOROUGH=UNKNOWN"  


# most are within the ideal 20MB - 2GB range
tibble(
  files = list.files("data/ems-nyc", recursive = TRUE),
  size_MB = file.size(file.path("data/ems-nyc", files)) / 1024 * 1024
)

### Import as Parquet

ems_nyc_parquet <- open_dataset("data/ems-nyc")

# in the future, join metadata to main file to describe these codes: 

ems_nyc_parquet |> 
  group_by(FINAL_CALL_TYPE) |> 
  count(sort = T) |> 
  collect()

ems_nyc_parquet |> 
  group_by(INCIDENT_DISPOSITION_CODE) |> 
  count(sort = T) |> 
  collect()

