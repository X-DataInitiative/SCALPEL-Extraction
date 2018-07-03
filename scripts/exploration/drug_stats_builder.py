# Imports:
from os import path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from exploration.events_flowchart import build_events_flowchart
from exploration.fall_study_stats import TherapeuticStats, PharmaStats, MoleculeStats
from matplotlib.backends.backend_pdf import PdfPages


def Drug_testing(metapath: str, flowpath: str, outpath: str, moulecule_mapping_csv: str,
                 therapeutic_level_exposures_parquet: str,
                 pharmaceutique_level_exposures_parquet: str, molecule_level_exposures_parquet: str):
    """
    This function creates bar charts for the purchase/exposure of drugs at the Therapeutic, Pharmaceutic, and Molecule level, using the provided input constraints.
    Parameters
    ----------
    metapath
        The filepath of Metadata in Json format
    flowpath
        The filepath of Flowchart_json
    outpath
        Path of folder where the output Pdfs are to be saved
    moulecule_mapping_csv
        The filepath of the Molecule mapper in CSV format
    therapeutic_level_exposures_parquet
        The filepath of the data at the Therapeutic level, in Parquet format
    pharmaceutique_level_exposures_parquet
         The filepath of the data at the Pharmaceutic level, in Parquet format
    molecule_level_exposures_parquet
        The filepath of the data at the Molecular level, in Parquet format

    ----------
    Outputs
        The constructed graphs in Pdf format, at the specified outpath.
    """

    molecule_mapping = pd.read_csv(moulecule_mapping_csv)
    sns.set_palette("muted")
    event_type = "achats"
    colors_dict = {
        "Antihypertenseurs": sns.color_palette()[0],  # Blue
        "Antidepresseurs": sns.color_palette()[1],  # Vert
        "Hypnotiques": sns.color_palette()[2],  # Rouge
        "Neuroleptiques": sns.color_palette()[3]  # Violet
    }
    therapeutic_level_exposures = spark.read.parquet(therapeutic_level_exposures_parquet)
    with open(metapath, 'r') as fp:
        metadata_json = json.load(fp)
    with open(flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    events = build_events_flowchart(therapeutic_level_exposures, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_therapeutique.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for therapeutic_level_exposures in events:
            therapeutic_stats = TherapeuticStats(therapeutic_level_exposures, colors_dict, event_type)
            fig = plt.figure()
            therapeutic_stats.plot()
            pdf.savefig(fig)
    pharmaceutique_level_exposures = spark.read.parquet(pharmaceutique_level_exposures_parquet)
    events = build_events_flowchart(pharmaceutique_level_exposures, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_pharmaceutique.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for pharmaceutique_level_exposures in events:
            pharma_stats = PharmaStats(molecule_mapping[["pharmaceutic_family", "therapeutic"]].drop_duplicates(),
                                       pharmaceutique_level_exposures, event_type, colors_dict)
            fig = plt.figure(figsize=(8, 5))
            pharma_stats.plot()
            plt.tight_layout()
            pdf.savefig(fig)
    molecule_level_exposures = spark.read.parquet(molecule_level_exposures_parquet)
    events = build_events_flowchart(molecule_level_exposures, flowchart_json, metadata_json)
    filepath = path.join(outpath, "{}_niveau_moleculaire.pdf".format(event_type))
    with PdfPages(filepath) as pdf:
        for molecule_level_exposures in events:
            molecule_stats = MoleculeStats(
                molecule_mapping[["pharmaceutic_family", "therapeutic", "molecule"]].drop_duplicates(),
                molecule_level_exposures, colors_dict, event_type)
            fig = plt.figure(figsize=(8, 5))
            ax = molecule_stats.plot_overall_top_molecules(top=30)
            plt.tight_layout()
            pdf.savefig(fig)

            fig = plt.figure(figsize=(8, 5))
            ax = molecule_stats.plot_top_of_therapeutic_classes()
            plt.tight_layout()
            pdf.savefig(fig)
