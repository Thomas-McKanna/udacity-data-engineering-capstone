"""
This file transforms the spark output data into javascript variables that can
easily be accessed from the web application.
"""

import re
import os
import glob
import shutil
import json


def _extract_json_from_spark_output(spark_folder: str):
    """
    The spark output data will be in three folders: covid_19_daily,
    covid_19_seven_day, and covid_19_thirty_day. Within each of these
    folders is a JSON file. This function renames that JSON file and moves
    it into the main project directory.

    Parameters:
    -----------
    spark_folder: one of the three spark output folder names

    Returns: name of the newly created file

    Note: this function can only be run after the spark script finishes and
    can only be run once, since it moves the spark JSON file.
    """
    new_file_name = f"{spark_folder}.js"
    json_file = glob.glob(f"{spark_folder}/*.json")[0]
    shutil.move(json_file, new_file_name)
    return new_file_name


def _read_in_file(input_file: str):
    """
    Reads in the file created by _extract_json_from_spark_output.

    Parameters:
    -----------
    input_file: one of covid_19_daily.json, covid_19_seven_day.json,
    or covid_19_thirty_day.json

    Returns: data contained within file
    """
    with open(input_file, "r") as f:
        return f.read()


def _remove_quotes(string: str):
    """
    Removes escaped brackets and quotes that spark produces in the output.
    It is not easy to remove these escaped characters in spark itself, so this
    step is done in postprocessing.

    Parameters:
    -----------
    string: the string returned by _read_in_file

    Returns: the cleaned up string
    """
    string = re.sub(r'"\[', "[", string)
    string = re.sub(r'\]"', "]", string)
    string = re.sub(r'\\"', '"', string)
    string = re.sub(r'"{', '{', string)
    string = re.sub(r'}"', '}', string)
    return string


def _make_into_array(string: str):
    """
    Combines the JSON objects produces by spark into an array.

    Parameters:
    -----------
    string: the string returned by _remove_quotes

    Returns: a string containing a properly formatted array of JSON objects
    """
    # all but last since spark writes a trailing newline character
    string = string.split("\n")[:-1]
    string = ','.join(string)
    return f"[{string}]"


def _write_pretty_print_javascript_variable_to_file(string: str, variable_name: str, outfile_name: str):
    """
    Transforms the JSON array into a JavaScript variable declaration and writes
    this string out to a file. This file is intended to be imported into the
    web application.

    Parameters:
    -----------
    string: the string returned by _make_into_array
    variable_name: the name the JavaScript variable should be
    outfile_name: the name of the outputfile that is written
    """
    data = json.loads(string)
    with open(outfile_name, "w") as f:
        f.write(f"var {variable_name} = {json.dumps(data, indent=4)}")


def remove_spark_folder(spark_folder: str):
    """
    Removes the spark folders that have now been processed.

    Parameters:
    -----------
    spark_folder: the name of the spark folder
    """
    shutil.rmtree(spark_folder)


def postprocess(spark_folder: str, javascript_variable_name: str):
    """
    Cleans up the spark data and writes it out as a JavaScript variable to a
    file.

    Parameters:
    -----------
    spark_folder: the name of a spark output folder
    javascript_variable_name: the name the JavaScript variable should be
    """
    file_name = _extract_json_from_spark_output(spark_folder)
    string = _read_in_file(file_name)
    string = _remove_quotes(string)
    string = _make_into_array(string)
    _write_pretty_print_javascript_variable_to_file(string, javascript_variable_name, file_name)
    remove_spark_folder(spark_folder)


spark_folders_to_process = [
    "covid_19_daily",
    "covid_19_seven_day",
    "covid_19_thirty_day"
]

javascript_variable_names = [
    "daily_data",
    "seven_day_data",
    "thirty_day_data"
]

if __name__ == "__main__":
    for f, v in zip(spark_folders_to_process, javascript_variable_names):
        postprocess(f, v)
