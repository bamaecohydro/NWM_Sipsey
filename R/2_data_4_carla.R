#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Title: NWM Exploratory Analysis
#Date: 5/19/2023
#Coder: Nate Jones (cnjones7@ua.edu)
#Purpose: Download NWM data for specific locations along the Sipsey
#Data: https://registry.opendata.aws/nwm-archive/

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 1: Setup workspace -------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Clear Memory
remove(list=ls())

#open libraries of interest
library(tidyverse)
library(lubridate)
library(ncdf4)
library(nhdplusTools)
library(sf)
library(parallel)
library(dataRetrieval)

# Load locations of interest 
sites <- read_csv("data/Atkinson_Sites_LatLong.csv")

#define period of study
dates <- seq(ymd('1979-10-01'), ymd('2020-09-30'), by = '1 day')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 2: Identify COMID for reach location
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Create function to find COMID based on lat and long
comid_fun <- function(n){
  #Define lat and long
  lat <- sites$Latitude[n]
  long <- sites$Longitude[n]
  
  #Define coordinate reference system
  crs <- '+proj=longlat +datum=WGS84 +no_defs'
  
  #Idnetify NHDPlus COMID
  comid <- st_point(c(lat, long)) %>% 
    st_sfc(., crs=crs) %>% 
    discover_nhdplus_id(., raindrop = T) %>% 
    filter(id == 'nhdFlowline')
  
  #Export COMID
  tibble(
    Site = sites$Site[n],
    comid = comid$comid)
}

#Remove site 14 because its throwing an error
sites <- sites %>% filter(Site != 'Bogue 1')

#Apply error
df_comid <- lapply(seq(1,nrow(sites)), comid_fun) %>%  bind_rows()

#Left join to sites data
sites <- left_join(sites, df_comid)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 3: Create Download Function ----------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#define comids of interst
comids <- sites$comid

#define inner func
nwm_data <- function(date, comids){
  
  #Identify web and local directory locations
  date_text<-date %>% format(., "%Y%m%d") %>% paste0(., "1200")
  year_text <- year(date)
  web_text<-paste0("https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/",
                   year_text, "/",date_text,".CHRTOUT_DOMAIN1.comp")
  dir_text<-paste0("data/",date_text,".CHRTOUT_DOMAIN1.comp")
  
  #Download flow data from the web
  download.file(web_text, dir_text, method ='libcurl', mode='wb')
  
  #Open NET cdf
  nc<-nc_open(dir_text)
  
  #Extrract Streamflow
  df<-tibble(
    reach_id = ncvar_get(nc, "feature_id"), 
    nwm_flow_cms = ncvar_get(nc, "streamflow"))
  
  #remove file
  nc_close(nc)
  file.remove(dir_text)
  
  #Filter based on comid
  df<-df %>% 
    filter(reach_id %in% comids) %>% 
    mutate(date=date)
  
  #Export file
  df
  
}

#Create error catching fun
error_fun <- function(n){
  tryCatch(
    nwm_data(
      date = dates[n],
      comids = comids), 
    error = function(e) tibble(reach_id  = NA, nwm_flow_cms =NA, date = dates[n]))
}

#Prep Clusters
n.cores<-detectCores()-1
cl<-makeCluster(n.cores)
clusterEvalQ(cl, {
  library(tidyverse)
  library(lubridate)
  library(ncdf4)
  library(nhdplusTools)
  library(sf)
})
clusterExport(cl, c("nwm_data", "dates", "comids"))

#Run in parallel
list_nwm<-parLapply(cl,seq(1, length(dates)),error_fun)

#Now, bind rows from list output
df_nwm<-list_nwm %>% bind_rows()

#Stop the clusters
stopCluster(cl)

#join to site name
df_nwm <- left_join(df_nwm %>% rename(comid = reach_id), df_comid)

#write csv
write.csv(df_nwm, "docs//sipsey.csv")

