# Data Engineering Capstone Project: Interactive Covid-19 Map

This repository contains the data and code used to create my capstone project for the Data Engineer Nanodegree through Udacity. The aim of this project is to create an interactive visualization to better understand the history of Covid-19 cases and deaths in the United States. 

The project uses Apache Spark to process publicly available Covid-19 data, aggregating by one-day, seven-day, and thirty-day rolling averages. Data for each interval is then assigned to one of ten buckets, indicating severity at that point in time. This aggregated data is then used to power a web application that displays the data on a Google Map. 

The user is able to use take the following actions:

- Use the slider to navigate through time
- Select the desired metric and time interval using radio buttons
- Click on any state on the map to get the specific metric count for the given day. 

The GIF below shows what the web application looks like:

<img src='https://github.com/Thomas-McKanna/udacity-data-engineering-capstone/raw/master/covid_map.gif' title='Video Walkthrough' width='1000' alt='Video Walkthrough' />

## Steps taken to complete this project

1. Retrieved raw Covid-19 data from https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data (Source: John Hopkins University)
2. Retrieved GeoJSON state shape data from https://eric.clst.org/tech/usgeojson/
3. Wrote PySpark Jupyter notebook to process data. This included cleaning the data, aggregating by three intervals (daily, seven day, thirty day), and formatting the data into a JSON format convient for post-processing.
4. Wrote a Python script to process the Spark output into a format readily usable by a JavaScript web application.
5. Wrote an HTML and a JavaScript file to present the processed data on a map and to allow the user to interact with the map.

## Purpose of the final data model

The final data model is several JavaScript variables containing an array of JSON objects. Three files are produced by the `post_processing.py` script:

1. `covid_19_daily.js`
2. `covid_19_seven_day.js`
3. `covid_19_thirty_day.js`

The purpose of this data model is to allow for easy metric and color lookup from the JavaScript web application.

### Data Dictionary

For each of the three output JavaScript files, the data format is the same. The data is comprised of an array of JSON objects. Each object has the form:

```
{
    "date": "2020-03-22",
    "values": [
        {
            "state": string,
            "confirmed": number,
            "deaths": number,
            "confirmed_color": string,
            "deaths_color": string
        },
        ...remaining 49 US states...
    ]
}
```

The "date" key contains the date of the Covid-19 statistics contained in "values". The "confirmed" and "deaths" keys contain the count of confirmed cases and deaths for the associated state and date. The "confirmed_color" and "deaths_color" keys contains the severity color that is used to color each state on the Google Map.

Here are the first several lines of `covid_19_daily.js`:

```js
var daily_data = [
    {
        "date": "2020-03-22",
        "values": [
            {
                "state": "Utah",
                "confirmed": 162,
                "deaths": 1,
                "confirmed_color": "#FFFED1",
                "deaths_color": "#FFFED1"
            },
            {
                "state": "Hawaii",
                "confirmed": 53,
                "deaths": 0,
                "confirmed_color": "#FFFED1",
                "deaths_color": "#FFFED1"
            },
            ...
```

## Project files

**Note: the raw data, when combined, has 1,590,041 rows.**

- `covid_19_daily_reports`: folder containing raw data files with Covid-19 data (**data source #1, csv format)**
- `etl.ipynb`: ETL Jupyter notebook for processing raw data (**contains data quality checks**)
- `post_processing.py`: Python script that transforms Spark output into JavaScript variables
- `states.geojson`: polygon data for each state so that Google Maps can draw and color states; correlated with data contained in JavaScript variables to match data with states on map (**data source #2, json format**)
- `interactive_map.html`: HTML for map and control elements
- `map_logic.js`: JavaScript logic for coloring map and responding to user interaction with HTML elements
- `stles.css`: stlying for HTML elements

## How to run scripts and view web application

Note: Docker, Python, and a Google Maps JavaScript API key are needed to complete these steps.

1. Clone the repo: `git clone https://github.com/Thomas-McKanna/udacity-data-engineering-capstone.git`
2. Go to repo: `cd udacity-data-engineering-capstone`
3. Start Jupyter notebook docker container: `docker run -it --rm -p 8888:8888 -v <path/to/repo>:/home/jovyan/work jupyter/pyspark-notebook`
4. Follow instructions in terminal to open up web interface to Jupyter notebook and open `etl.ipynb`
5. Run all parts of notebook to produce Spark output folders
6. Run post-processing script: `python3 post_processing.py`
7. Insert Google Maps API key in `interactive_map.html`
8. Open `interactive_map.html` in a web browser

## Addressing other scenarios

- If the data size was increased by 100x, the following actions _may_ need to be taken. First, I would migrate from a Jupyter notebook to a dedicated PySpark script that can be submitted to a Spark cluster. The notebook contains several statements that are only there for exploratory purposes and add unnecessary computation time. Second, I would deploy a Spark cluster on a cloud service like AWS so that computation is parellized and thus faster. The notebook takes about 2 minutes to run on my personal computer, so increasing 100x would take around 200 minutes (~3 hours), which is a long time, but still feasible on a personal computer.
- If the pipeline would be run every morning, then I would write an Airflow DAG to achieve this requirement. I would configure the DAG to run every morning and only process the Covid-19 data for the previous day. There would be steps for pulling the latest day of data, rolling up that data by day, and then appending to the JavaScript variable files.
- If the data needed to be accessed by 100+ people at once, I would configure a load-balanced web server, possibly with several backend servers, so that the users would not have to wait in a long queue to download the data contained within the JavaScript files.

## Defending choice of tools, technologies, and data model

- A Jupyter notebook was used to run the Spark code in local mode since the size of the data (~1.5 million rows) was small enough to run fairly fast. Using a notebook made it easy to debug my code and verify expected results.
- Docker was used to access the Jupyter notebook, which meant I had to spent no time configuring Spark myself.
- A Python post-processing script was written because I was having a difficult time formatting data properly using PySpark. Given that the aggregated data files are each only 4.5 MB, using a Python script for this purpose was works well and runs very quickly.
- I decided to store my "database" as JavaScript variables because the size of the summarized data only totaled around 15 MB, which is small enough to be quickly downloaded by a web browser. By using this approach, the user is handed all the data at once, which allows the map to be updated with almost no delay as the slider is moved on the web application.
- I opted to use a simple HTML file and JavaScript file rather than a web framework because the complexity of this project did not warrant the overhead of a web framework. Basic JavaScript was enough to get the job done.
- I used the Google Maps JavaScript API because it contains functions for easily rendering GeoJSON data (the state shapes) and coloring the states.

## Addendum

There were several obstacles that I had to overcome to complete this project. I have them listed here for my future reference:

- Since the "date" field in the raw data was stored in the file name and not the contained data, I had to learn how to use the PySpark `input_file_name()` function to select the filename as a column in a DataFrame.
- When I began to aggregate numeric values, I began to received unexpected answers. It turns out that all of my numeric columns were being considered strings. So, I converted all of these columns to integers.
- I discovered that the files up until and including 03/21/2020 do not have data for US counties (it is instead broken down by country). This is likely because data for all US counties was not being collected up until that point. So, my data processing only takes into account data from 03/22/2020 and onwards. This is not a big problem, since the case numbers are all very small before 03/22/2020, and would likely be in the lowest bucket anyway.
- Originially, I was trying to aggregate data by US county instead of US state. It turned out that aggregating by county produced too much data (~600MB) to be easily presented in a web browser and the web application would become unreponsive. So I changed the ETL code to aggregate by state instead (~15 MB).
- The data contained US "state" data for several non-state entities, such as "Recovered", "District of Columbia", and "Puerto Rico". I removed these "states" from the data.
- I discovered that the "confirmed" cases number is an accumulating counter of the number of cases in each state. To get a better idea of the intensity of the pandemic at any given point, I switched to using delta values by employing the PySpark `lag` function.
- I discovered that the "active" field only seems to have data available early on during the pandemic. So I switch to only aggregating the "confirmed" and "deaths" fields.
- One obstacle encountered when splitting records into buckets was that the record with the maximum confirmed case value had a *very* large value compared to the majority of records. To get around this, I instead used the 95th percentile of the confirmed/deaths value.
- My window function was not using a slideDuration, which caused much of the rolled up data to be missing.
- My ETL notebook outputs three JSON files, but I realized that the output for each is a directory containing many files. I needed to coalesce into one partition.
- I followed the guide at https://simplemaps.com/resources/shapes-google-maps to better understand how to display GeoJSON data on a Google Map and to color GeoJSON features. 
