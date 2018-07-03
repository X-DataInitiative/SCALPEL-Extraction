# IMPORTS:
import json

import pandas as pd
import pyspark.sql.functions as fn
from exploration.events_flowchart import build_events_flowchart
from exploration.fall_study_stats import save_fractures_stats


def fracture_stat_chart(metapath: str, flowpath: str, outpath: str, hospath: str, private_amb_path: str,
                        public_amb_path: str, liberal_path: str, start_date: str, end_date: str):
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
    hospit = spark.read.parquet(hospath)
    private_amb = spark.read.parquet(private_amb_path)
    public_amb = spark.read.parquet(public_amb_path)
    liberal = spark.read.parquet(liberal_path)
    outcomes = hospit.union(private_amb).union(public_amb).union(liberal)
    outcomes = outcomes.filter(fn.col("start").between(pd.to_datetime(start_date), pd.to_datetime(end_date)))
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    events = build_events_flowchart(outcomes, flowchart_json, metadata_json)
    for i, fractures in enumerate(events):
        file_path = path.join(outpath, "fractures_stats_apres_etape_{}".format(i))
        save_fractures_stats(file_path, fractures)
