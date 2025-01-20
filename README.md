# arrow-r 

## Background 

In organizations with limited resources and siloed teams, data sharing is a clunky process. 
This is especially the case when one department produces a large file (say, 12GB), and another team must 
provide analysis within a tight deadline.  

These instances inspired me to explore the possibilities of Apache Arrow. I utilized the following resources:  

* [R for Data Science Chapter 22: Arrow](https://r4ds.hadley.nz/arrow#getting-the-data) by Hadley Wickham 
* [Doing More With Data: An Introduction to Arrow for R Users](https://youtu.be/O42LUmJZPx0?si=ENeW8Ihz_BU8L2tG) by Danielle Navarro at Voltron Data  
* [Using the {arrow} and {duckdb} packages to wrangle medical datasets that are Larger than RAM](https://youtu.be/Yxeic7WXzFw?si=t4Dxvy-UdS__EUIf) by Peter Higgins at R Consortium 


## Data 

The Fire Department of New York City (FDNY) maintains data produced by their EMS dispatch system. 
The **[EMS Incident Dispatch Data](https://data.cityofnewyork.us/Public-Safety/EMS-Incident-Dispatch-Data/76xm-jjuj/about_data)** file contains 27M records with information relating to incident location, 
perceived call severity, and Fire Department response time.


## R Version

This project was produced with R version 4.3.1.
