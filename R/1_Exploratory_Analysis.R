#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Title: NWM Exploratory Analysis
#Date: 3/4/2022
#Coder: Nate Jones (cnjones7@ua.edu)
#Purpose: Extract modeled streamflow along the Sipsey River
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#NWM data: https://registry.opendata.aws/nwm-archive/

#NHD Plus Tools: https://usgs-r.github.io/nhdplusTools/index.html

#Steps (for each timestep)
# 1. Download NWM data 
# 2. Open Data
# 3. Define NHDPlus reach


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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 2: Create Download Function ----------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#Create function to download flow data based on date, lat, and long
nwm_data<-function(date, lat, long, crs){

  #Idnetify NHDPlus COMID
  comid <- st_point(c(lat, long)) %>% 
    st_sfc(., crs=crs) %>% 
    discover_nhdplus_id(.)
  
  #Identify web and local directory locations
  date_text<-date %>% format(., "%Y%m%d") %>% paste0(., "1200")
  year_text <- year(date)
  web_text<-paste0("https://noaa-nwm-retrospective-2-1-pds.s3.amazonaws.com/model_output/",
                   year_text, "/",date_text,".CHRTOUT_DOMAIN1.comp")
  dir_text<-paste0("data/",date_text,".CHRTOUT_DOMAIN1.comp")
  
  #Download flow data from the web
  download.file(web_text, dir_text, method ='libcurl', mode='wb')
  
  #oPEN NET Cdf
  nc<-nc_open(dir_text)
  
  #Extrract Streamflow
  df<-tibble(
    reach_id = ncvar_get(nc, "feature_id"), 
    streamflow_cms = ncvar_get(nc, "streamflow"))
  
  #remove file
  nc_close(nc)
  file.remove(dir_text)
  
  #Filter based on comid
  df<-df %>% 
    filter(reach_id %in% comid) %>% 
    mutate(date=date)
  
  #Export file
  df

}


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 3: Apply Download Function -----------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#create date input
dates<-seq(ymd('1999-10-01'),ymd('2000-09-30'), by = '1 day')

#create wrapper fun
fun<-function(n){
  tryCatch(
    nwm_data(
      date = dates[n],
      lat = -87.7764022,
      long = 33.25706249,
      crs = 4269
    ), 
    error = function(e) tibble(reach_id  = NA, streamflow_cms =NA, date = dates[n]))
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
clusterExport(cl, c("nwm_data", "dates"))

#Run in parallel
output<-parLapply(cl,seq(1, length(dates)),fun)

#Now, bind rows from list output
output<-output %>% bind_rows()

#Stop the clusters
stopCluster(cl)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 4: Compare to USGS data --------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Download USGS Data
df<-readNWISdv(siteNumbers = '02446500', 
               parameterCd = '00060', 
               startDate = '1999-10-01', 
               endDate = '2000-09-30')

#Tidy Data
df<-df %>% 
  select(date = Date, 
         flow = X_00060_00003) %>% 
  mutate(date = ymd(date), 
         nwis_flow_cms = 0.0283168*flow) %>% 
  select(date, nwis_flow_cms)

#Combine without output
output<- output %>% rename(nwm_flow_cms = streamflow_cms) %>% select(date, nwm_flow_cms)
df<-left_join(df, output) %>% as_tibble()

#plot
df %>% 
  ggplot()+
    geom_line(aes(x=date,y=nwis_flow_cms), col="blue", lwd=1.2) +
    geom_line(aes(x=date,y=nwm_flow_cms), col="red", lwd=1.2) +
    #Plot y-axis in log scale
    scale_y_log10() +
    #Add predefined black/white theme
    theme_bw() +
    #Change font size of axes
    theme(
      axis.title = element_text(size = 14), 
      axis.text  = element_text(size = 10)
    ) + 
    #Add labels
    xlab(NULL) + 
    ylab("Flow [cfs]")
