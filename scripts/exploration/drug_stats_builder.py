# Imports:
import json
from os import path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from exploration.events_flowchart import build_events_flowchart
from exploration.exposures_stats import save_exposures_stats
from exploration.fall_study_stats import TherapeuticStats, PharmaStats
from matplotlib.backends.backend_pdf import PdfPages
from pyspark.sql import *


def exposures_chart_builder_therapeutic(metapath: str, flowpath: str, outpath: str):
    """
    This function creates bar charts for the exposure of drugs using the provided input constraints.
    Parameters
    ----------
    metapath
        The filepath of Metadata in Json format
    flowpath
        The filepath of Flowchart_json
    outpath
        Path of folder where the output Pdfs are to be saved

    ----------
    Outputs
        The constructed graphs in Pdf format, at the specified outpath.
    """

    sns.set_context("notebook")
    sns.set_style("darkgrid")
    sns.set_palette("muted")
    event_type = "expositions"
    colors_dict = {
        "Antihypertenseurs": sns.color_palette()[0],  # Blue
        "Antidepresseurs": sns.color_palette()[1],  # Vert
        "Hypnotiques": sns.color_palette()[2],  # Rouge
        "Neuroleptiques": sns.color_palette()[3]  # Violet
    }
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    exposures_path, purchases_path = retrieve_data_filepath(metadata_json)
    drug_exposures = pull_data(exposures_path)
    metadata_json = json.dumps(metadata_json)
    events = build_events_flowchart(drug_exposures, flowchart_json, metadata_json)
    file_path = path.join(outpath, "{}_niveau_therapeutique.pdf".format(event_type))
    with PdfPages(file_path) as pdf:
        for i, drug_exposures in enumerate(events):
            stats = TherapeuticStats(drug_exposures, colors_dict, event_type)
            fig = plt.figure()
            stats.plot()
            pdf.savefig(fig)
            duration_file_path = path.join(outpath, "exposures_duration_stats_step_{}.pdf".format(i))
            save_exposures_stats(duration_file_path, drug_exposures)


def drug_purchases_chart_builder_therapeutic(metapath: str, flowpath: str, outpath: str):
    """
    This function creates bar charts for the purchase of drugs using the provided metadata.
    Parameters
    ----------
    metapath
        The filepath of Metadata in Json format
    flowpath
        The filepath of Flowchart_json
    outpath
        Path of folder where the output Pdfs are to be saved

    ----------
    Outputs
        The constructed graphs in Pdf format, at the specified outpath.
    """

    sns.set_context("notebook")
    sns.set_style("darkgrid")
    sns.set_palette("muted")
    event_type = "achats"
    colors_dict = {
        "Antihypertenseurs": sns.color_palette()[0],  # Blue
        "Antidepresseurs": sns.color_palette()[1],  # Vert
        "Hypnotiques": sns.color_palette()[2],  # Rouge
        "Neuroleptiques": sns.color_palette()[3]  # Violet
    }
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    exposures_path, purchases_path = retrieve_data_filepath(metadata_json)
    drug_purchases = pull_data(purchases_path)
    metadata_json = json.dumps(metadata_json)
    events = build_events_flowchart(drug_purchases, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_therapeutique.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for drug_purchases in events:
            stats = TherapeuticStats(drug_purchases, colors_dict, event_type)
            fig = plt.figure()
            stats.plot()
            pdf.savefig(fig)


def retrieve_data_filepath(metadata_json):
    """

    Parameters
    ----------
    metadata_json: The Metadata Dict Variable

    Returns
    -------
    The function retrieves and returns the filepaths for the Exposures and drug Purchases data
    """
    exposures_path = purchases_path = 'null'
    for items in metadata_json["operations"]:
        if "drug_purchases" in items["name"]:
            purchases_path = items["output_path"]
    for items in metadata_json["operations"]:
        if "exposures" in items["name"]:
            exposures_path = items["output_path"]
    if exposures_path == 'null' or purchases_path == 'null':
        raise Exception("Your Metadata is incomplete")
    return [exposures_path, purchases_path]


def pull_data(path: str):
    """

    Parameters
    ----------
    path: The filepath for the data to be retrieved

    Returns
    -------
    The data from the specified filepath
    """
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.parquet(path)
    return data


def drug_purchases_chart_builder_pharma(metapath: str, flowpath: str, outpath: str, molecular_mapping_path: str):
    """
    This function creates bar charts for the purchase of drugs using the provided metadata.
    Parameters
    ----------
    molecular_mapping_path
        The filepath for the Molecule Mapping CSV
    metapath
        The filepath of Metadata in Json format
    flowpath
        The filepath of Flowchart_json
    outpath
        Path of folder where the output Pdfs are to be saved

    ----------
    Outputs
        The constructed graphs in Pdf format, at the specified outpath.
    """
    molecule_mapping = pd.read_csv(molecular_mapping_path)
    sns.set_context("notebook")
    sns.set_style("darkgrid")
    sns.set_palette("muted")
    event_type = "achats"
    colors_dict = {
        "Antihypertenseurs": sns.color_palette()[0],  # Blue
        "Antidepresseurs": sns.color_palette()[1],  # Vert
        "Hypnotiques": sns.color_palette()[2],  # Rouge
        "Neuroleptiques": sns.color_palette()[3]  # Violet
    }
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    exposures_path, purchases_path = retrieve_data_filepath(metadata_json)
    drug_purchases = pull_data(purchases_path)
    metadata_json = json.dumps(metadata_json)
    events = build_events_flowchart(drug_purchases, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_pharmaceutique.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for drug_purchases in events:
            stats = PharmaStats(molecule_mapping[["pharmaceutic_family", "therapeutic"]].drop_duplicates(),
                                drug_purchases, event_type, colors_dict)
            fig = plt.figure()
            stats.plot()
            pdf.savefig(fig)


def exposures_chart_builder_pharma(metapath: str, flowpath: str, outpath: str, molecular_mapping_path: str):
    """
    This function creates bar charts for the purchase of drugs using the provided metadata.
    Parameters
    ----------
    molecular_mapping_path
        The filepath for the Molecule Mapping CSV
    metapath
        The filepath of Metadata in Json format
    flowpath
        The filepath of Flowchart_json
    outpath
        Path of folder where the output Pdfs are to be saved

    ----------
    Outputs
        The constructed graphs in Pdf format, at the specified outpath.
    """
    molecule_mapping = pd.read_csv(molecular_mapping_path)
    sns.set_context("notebook")
    sns.set_style("darkgrid")
    sns.set_palette("muted")
    event_type = "expositions"
    colors_dict = {
        "Antihypertenseurs": sns.color_palette()[0],  # Blue
        "Antidepresseurs": sns.color_palette()[1],  # Vert
        "Hypnotiques": sns.color_palette()[2],  # Rouge
        "Neuroleptiques": sns.color_palette()[3]  # Violet
    }
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    exposures_path, purchases_path = retrieve_data_filepath(metadata_json)
    exposures = pull_data(exposures_path)
    metadata_json = json.dumps(metadata_json)
    events = build_events_flowchart(exposures, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_pharmaceutique.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for i, exposures in enumerate(events):
            stats = PharmaStats(molecule_mapping[["pharmaceutic_family", "therapeutic"]].drop_duplicates(), exposures,
                                event_type, colors_dict)
            fig = plt.figure(figsize=(8, 5))
            stats.plot()
            pdf.savefig(fig)
            duration_file_path = path.join(outpath, "exposures_duration_pharma_step_{}.pdf".format(i))
            save_exposures_stats(duration_file_path, exposures)
