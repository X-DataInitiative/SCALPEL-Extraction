import json
import os
from typing import Dict, List

from pyspark.sql import DataFrame

from .flowchart import parse_metadata, map_metadata_to_cohorts, flowchart_file_parser, read_data_frame, Cohort


def build_events_flowchart(events: DataFrame, flowchart_json: str,
                           metadata_json: str) -> List[DataFrame]:
    """
    Take a flowchart and creates a list with events that are filtered according to each
    step of the flowchart.
    :param events: Spark Dataframe that contains the events
    :param flowchart_json: json Flowchart.
    :param metadata_json: json Metadata.
    :return: List of Spark DataFrame.
    """
    metadata = parse_metadata(metadata_json)
    cohorts = map_metadata_to_cohorts(metadata)

    steps = flowchart_file_parser(flowchart_json, cohorts)

    return [events.join(cohort.patients.select("patientID"), "patientID", how="inner")
            for (_, cohort) in steps.items()]


def build_events_using_steps(events: DataFrame, steps: Dict[str, Cohort]) -> List[DataFrame]:
    """
    Create a list of Dataframe of Events that are based upon the Steps Cohorts. Each Dataframe is then the join between
    the cohort and the dataframe.
    :param events: Spark Dataframe that contains the events
    :param steps: The Steps Cohorts
    :return: List of Spark DataFrame
    """
    return [events.join(cohort.patients.select("patientID"), "patientID", how="inner")
            for (_, cohort) in steps.items()]


def flowchart_like_for_events(flowchart_json: str, metadata_json: str,
                              output_root: str,
                              mapping: List):
    """
    Takes a metadata and builds the flowcharts for all Event Dataframes at once using a list of tuples with event names
    as first argument and the corresponding plotting function as second argument
    :param flowchart_json: json Flowchart
    :param metadata_json: json Metadata
    :param output_root: Path of the directory where result PDFs are to be stored
    :param mapping: List of Tuples where the first element is the name of the Event Dataframe in the Metadata and the
                    second argument is the function that buillds the stats for that specific Event.
    """
    metadata = parse_metadata(metadata_json)
    cohorts = map_metadata_to_cohorts(metadata)
    steps = flowchart_file_parser(flowchart_json, cohorts)
    meta = json.loads(metadata_json)
    for (event_name, plotting_function) in mapping:
        for items in meta["operations"]:
            if event_name in items["name"]:
                filepath = items["output_path"]
        event = read_data_frame(filepath)
        flowcharted_events = build_events_using_steps(event, steps)

        for i, result in enumerate(flowcharted_events):
            output_filepath = os.path.join(output_root, event_name, "{}_stats_after_{}".format(event_name, i))
            plotting_function(output_filepath, result)
