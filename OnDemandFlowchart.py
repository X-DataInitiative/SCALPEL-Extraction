#IMPORTS:
import sys, os, json
import numpy
from exploration.flowchart import on_demand_flowchart
#Define a function which takes Metadata_json, Flowchart_json, and Output Filepath as input, and returns the prepared flowcharts using the file from the input filepaths using the "on_demand_flowchart" function
def getchart(metapath: str, flowpath: str, outpath: str):
        with open(metapath, 'r') as fp:
            metadata_json = json.load(fp)
        with open(flowpath, 'r') as fp:
            flowchart_json = json.load(fp)
        return [on_demand_flowchart(metadata_json, flowchart_json, outpath)]