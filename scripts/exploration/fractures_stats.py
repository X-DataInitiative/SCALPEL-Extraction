# IMPORTS:
import json
from os import path

import pandas as pd
import pyspark.sql.functions as fn
from exploration.events_flowchart import build_events_flowchart
from exploration.fall_study_stats import save_fractures_stats
from pyspark.sql import *


def fracture_stat_chart(metapath: str, flowpath: str, outpath: str, start_date: str, end_date: str):
    """
    The function constructs the bar graphs for the fracture stats, withn a specified time constraint, and saves them to the specified output directory.
    Parameters
    ----------
    metapath: The filepath for metadata in Json format
    flowpath: The filepath for Flowchart_json
    outpath: The path to the folder where the output graphs are to be saved
    hospath: The filepath for hospital stats in Parquet format
    private_amb_path: The filepath for Private Ambulance stats in Parquet format
    public_amb_path: The filepath for Public Ambulance stats in Parquet format
    liberal_path: The filepath for Liberal stats in Parquet format
    start_date: The date since when the stats are to be analysed. Input in format YYYY-MM-DD
    end_date: The date till when the stats are to be analysed. Input in format YYYY-MM-DD
    """

    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    hospath, liberal_path, private_amb_path, public_amb_path = scan_metadata(metadata_json)
    outcomes = merge_path(hospath, private_amb_path, public_amb_path, liberal_path)
    outcomes = outcomes.filter(fn.col('start').between(pd.to_datetime(start_date), pd.to_datetime(end_date)))
    metadata_json = json.dumps(metadata_json)
    events = build_events_flowchart(outcomes, flowchart_json, metadata_json)
    for i, fractures in enumerate(events):
        file_path = path.join(outpath, "fractures_stats_apres_etape_{}".format(i))
        save_fractures_stats(file_path, fractures)


def merge_path(hospath: str, private_amb_path: str, public_amb_path: str, liberal_path: str):
    """
    The function reads and mergers the data for all kinds of fractures
    Parameters
    ----------
    hospath: The filepath for the Hospitalized fractures data
    private_amb_path: The filepath for the Private Ambulatory Fractures data
    public_amb_path: The filepath for the Public Ambulatory fractures data
    liberal_path: The filepath for the Liberal Fractures data

    Returns
    -------
    The merged data.
    """
    spark = SparkSession.builder.getOrCreate()
    hospit = spark.read.parquet(hospath)
    private_amb = spark.read.parquet(private_amb_path)
    public_amb = spark.read.parquet(public_amb_path)
    liberal = spark.read.parquet(liberal_path)
    outcomes = hospit.union(private_amb).union(public_amb).union(liberal)
    return outcomes


def scan_metadata(metadata_json):
    """
    The function extracts the filepaths for various fractures data from the input Metadata. IN case the metadata is incomplete, throws exception and stops.
    Parameters
    ----------
    metadata_json: The Metadata in JSON format

    Returns
    -------
    The filepaths for different kinds of fractures.
    """
    liberal_path = hospath = private_amb_path = public_amb_path = 'null'
    for items in metadata_json["operations"]:
        if "liberal_fractures" in items["name"]:
            liberal_path = items["output_path"]
    for items in metadata_json["operations"]:
        if "hospitalized_fractures" in items["name"]:
            hospath = items["output_path"]
    for items in metadata_json["operations"]:
        if "public_ambulatory_fractures" in items["name"]:
            public_amb_path = items["output_path"]
    for items in metadata_json["operations"]:
        if "private_ambulatory_fractures" in items["name"]:
            private_amb_path = items["output_path"]
    if liberal_path == 'null' or hospath == 'null' or public_amb_path == 'null' or private_amb_path == 'null':
        raise Exception("Your Metadata is incomplete")
    return [hospath, liberal_path, private_amb_path, public_amb_path]
