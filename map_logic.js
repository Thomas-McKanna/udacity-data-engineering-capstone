// Reference to the Google Map
var map;

// Reference to a shared information window that updates based on where the
// user clicks on the map
var infoWindow;

// Variables that hold the state of the web application
var current_data = daily_data;
var current_metric = "confirmed";

// References to HTML elements
let slider = document.querySelector("#day-range");
let dateLabel = document.querySelector("#date");
let metricRadioGroup = document.querySelectorAll("input[name='metricRadioGroup']");
let intervalRadioGroup = document.querySelectorAll("input[name='intervalRadioGroup']");


// Gets run when the Google Map finishes loading
function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
        zoom: 5,
        center: {
            lat: 38.0000,
            lng: -98.5795
        }
    });

    map.data.loadGeoJson('states.geojson');

    setDaysIntoPandemic(0);

    infoWindow = new google.maps.InfoWindow();

    // Set up the event listener which displays the metric value when the
    // user clicks on the map
    map.data.addListener('click', function(event) {
        let state = event.feature.getProperty("NAME");
        let sliderValue = Number(slider.value);
        var count = getValueBasedOnStateAndDay(state, sliderValue, false);
        let html = 'Count: ' + count;
        infoWindow.setContent(html);
        infoWindow.setPosition(event.latLng);
        infoWindow.setOptions({ pixelOffset: new google.maps.Size(0, -30) });
        infoWindow.open(map);
    });
}


// Recolors the map based on the number of days into the Covid-19 pandemic
// and based on the current selection of interval and metric.
//
// Parameters:
// -----------
// days: the numeric number of days since March 22, 2020, which is when the
// data for this project begins
function setDaysIntoPandemic(days) {
    map.data.setStyle(function(feature) {
        let state = feature.getProperty('NAME');
        var color = getValueBasedOnStateAndDay(state, days, true);

        return {
            fillColor: color,
            fillOpacity: 0.8,
            strokeWeight: 1
        }
    });
}


// Changes the state variable containing the current metric selection based
// on the value of the clicked radio button
function changeMetric(e) {
    let metric = e.target.value;
    if (metric == "confirmed") {
        current_metric = "confirmed";
    } else if (metric == "deaths") {
        current_metric = "deaths";
    }
    // trigger slider event to update map
    slider.dispatchEvent(new Event("input"));
}


// Changes the state variable containing the current interval selection based
// on the value of the clicked radio button
function changeInterval(e) {
    let interval = e.target.value;
    if (interval == "daily") {
        current_data = daily_data;
    } else if (interval == "sevenday") {
        current_data = seven_day_data;
    } else if (interval == "thirtyday") {
        current_data = thirty_day_data;
    }
    // trigger slider event to update map
    slider.dispatchEvent(new Event("input"));
}


// Utility function to set event listeners for radio buttons
//
// Parameters:
// -----------
// group: reference to the radio button group (from querySelectorAll)
// callback: the function to call when a radio button is pressed in this group
function addEventListenersForRadioButtonGroup(group, callback) {
    group.forEach(
        function(rb) {
            rb.addEventListener("click", callback, false);
        }
    );
}


// Utility function to get metric value or color value based on desired state
// and days into the pandemic.
// 
// Parameters:
// -----------
// state: the state to get a value for
// day_index: the number of days into the pandemic to get a value for
// get_color: boolean value, if true get the color instead of the metric
function getValueBasedOnStateAndDay(state, day_index, getColor) {
    let data = current_data[day_index];
    var metric_label = current_metric;
    if (getColor) {
        metric_label = metric_label + "_color";
    }

    for (index in data["values"]) {
        let entry = data["values"][index];
        if (entry["state"] == state) {
            return entry[metric_label];
        }
    }
}


// Sets all the event handlers for the web application
function setEventHandlers() {
    slider.addEventListener("input", function() {
        let sliderValue = Number(slider.value);
        dateLabel.innerHTML = daily_data[sliderValue]["date"];
        setDaysIntoPandemic(sliderValue);
    }, false);

    //
    addEventListenersForRadioButtonGroup(metricRadioGroup, changeMetric);
    addEventListenersForRadioButtonGroup(intervalRadioGroup, changeInterval);
}


setEventHandlers()