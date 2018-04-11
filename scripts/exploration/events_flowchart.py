from typing import List

from pyspark.sql import DataFrame

from exploration.flowchart import flowchart_file_parser, map_metadata_to_cohorts, \
    parse_metadata


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
