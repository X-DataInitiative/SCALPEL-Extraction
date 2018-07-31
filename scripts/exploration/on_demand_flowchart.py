# IMPORTS:
import json
from exploration.flowchart import on_demand_flowchart


def getchart(metapath: str, flowpath: str, outpath: str):
    """
    Takes the filepath for metadata and flowchart json and builds flowcharts corresponding to them. Outputs Pdfs of the charts.
    Parameters
    ----------
    metapath
           Filepath to the metadata
    flowpath
           Filepath of the Flowchart Json
    outpath
           Filepath for the output Pdfs

    Returns :
    -------
           The PDFs for the generated graphs, and saves them to the outpath.
    """
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    return {on_demand_flowchart(metadata_json, flowchart_json, outpath)}
