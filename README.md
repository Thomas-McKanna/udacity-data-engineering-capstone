# Data Engineering Capstone Project

<img src='https://github.com/Thomas-McKanna/udacity-data-engineering-capstone/raw/master/covid_map.gif' title='Video Walkthrough' width='300' alt='Video Walkthrough' />

## Steps taken to generate map boundaries

I followed the guide at https://simplemaps.com/resources/shapes-google-maps. I downloaded shapefile data from https://catalog.data.gov/dataset/tiger-line-shapefile-2019-nation-u-s-current-county-and-equivalent-national-shapefile. The result of the guide was a GeoJSON file containing map boundaries by county.

## Data cleaning obstacles

I had to decide how to transform the data. I decided on using a Jupyter notebook docker image with Apache Spark pre-installed, since this minimized effort spent setting up my work environment.

Once the Jupyter notebook environment was up and running, I had to load the input dataset into a dataframe. This was simple enough to do using the spark `read.csv` function, but what I did not realize is that a key field of my data would be missing: the date of each record. This is because the records are stored in separate files, one for each month. So, the date information is stored in the file name rather than the data itself.

When I began to aggregate numeric values, I began to received unexpected answers. It turns out that all of my numeric columns were being considered strings. So, I converted all of these columns to integers.

I discovered that the files up until and including 03/21/2020 do not have data for US counties (it is instead broken down by country). This is likely because data for all US counties was not being collected up until that point. So, my data processing will only take into account data from 03/22/2020 and onwards. This is not a big problem, since the case numbers are all very small before 03/22/2020, and would be in the lowest bucket anyway.

It turned out that aggregating by county produced data too big to be easily processed by tools that convert GeoJSON to image files (it quickly became apparent that using Google Maps on the fly would not be feasible since the browser became unresponsive when loading just one of the datasets due to memory issues). So I changed the ETL code to aggregate by state instead.

The data contained US "states" entries for "Recovered" and "District of Columbia", as well as others. I removed these "states" from the data.

I discovered that the "confirmed" cases number is an accumulating counter of the number of cases in each state. To get a better idea for the intensity of the pandemic at any given point, I switched to using delta values.

I discovered that the "active" field only seems to have data available early on during the pandemic.

## Hex gradient

Least severe to most severe:
#FFFED1
#FFE98F
#FFDF8F
#FDC979
#FFAA61
#FF9161
#FF7149
#E55C36
#D84E27
#CB2921

One obstacle encountered when splitting records into buckets was that the record with the maximum confirmed case value had a *very* large value compared to the majority of records. To get around this, I instead used the 95th percentile of the confirmed case value.

## Processing data

My window function was not using a slideDuration, which caused much of the rolled up data to be missing.

## Output files

My ETL notebook outputs three JSON files, but I realized that the output for each is a directory containing many files. It seems that these JSON files are being split into many files. I needed to coalesce into one partition.