		Gage Station List Readme File	

Table of Contents

    Introduction 
    What type of metadata is there for the sites, and what is the format ?
    What data are available?
    References related to SHEF processing
    Who do I contact if I have a question ?


INTRODUCTION

The Climate Prediction Center (CPC), a component of the National Centers
for Environmental Prediction (NCEP) acquires gage-based precipitation
reports in near real-time from several thousand sites across the contiguous
48 states of the USA.  These data support a variety of NCEP tasks, 
especially the prototype, real-time, hourly, multi-sensor National 
Precipitation Analysis (NPA), a joint effort of NCEP and the NWS Office
of Hydrology (OH).

Approximately 3000 automated, hourly rain gage observations are available
over the contiguous 48 states via the GOES Data Collection Platform (DCP)
administered by OH.  These hourly reports are transmitted continuously
throughout the day in groups of 3 or 4 reports per transmission to NCEP
in a SHEF-encoded message.

Meanwhile, there are approximately 5,800 daily rain gage reports 
per day out of a total available set of 9,000. These daily precipitation
reports are collected by the 12 River Forecast Centers (RFC) and sent to 
NCEP via AFOS in a SHEF-encoded message.  These sites are predominately 
cooperative sites in that a majority also belong to the National Climatic
Data Center (NCDC) cooperative reporting network.  However, many other 
sites belong to networks that do not fall within the standard cooperative 
network grouping.


WHAT TYPE OF METADATA IS THERE FOR THE SITES AND WHAT IS THE FORMAT ?

Most of the metadata for these sites are the geographical coordinates
of the site, elevation (if known) and the city, state names.  The primary
site ID is based upon the National Weather Service Communications 
Handbook #5.  For those sites without such a ID, the NESDIS platform
ID may be used, or lastly an ID given to the site by the owner of the
network.  In most cases, the secondary ID for a site is the NCDC 
cooperative network ID or NESDIS platform ID.

    COLUMNS      FIELD NAME

     1 -  8      Primary Site ID
    10 - 15      Latitude   (Real number with 2 decimal places 0-90)
    17 - 22      Longitude  (Real number with 2 decimal places 0-180)
    24 - 31      Station Type
    33 - 41      Secondary Site ID (-- = no value for this field)
    44 - 48      Elevation  (Integer number, feet above MSL)
    50 - 51      State      (Post Office Abbreviation)
    53 - 78      Place Name (City or town name)

WHAT DATA ARE AVAILABLE? 
 
See the readme file for the GCIP/EOP Surface: NCEP Ancillary Catalogue of 
Available GCIP Precipitation Data (NCEP/CPC). 

REFERENCES RELATED TO SHEF PROCESSING

...Standard Hydrometeorological Exchange Format (SHEF), Version 1.2
   Hydrology Handbook No. 1, Office of Hydrology, National Weather Service


WHO TO CONTACT IF YOU HAVE QUESTIONS

	Stage IV analysis : Mike.Baldwin@noaa.gov
	Gage-only, multi-sensor techniques : Dongjun.Seo@noaa.gov
	Precipitation data: Sid.Katz@noaa.gov
