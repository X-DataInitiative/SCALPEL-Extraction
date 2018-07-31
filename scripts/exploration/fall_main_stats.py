import argparse
import functools
import json
from typing import List, Tuple
import matplotlib

matplotlib.use('agg')

import seaborn as sns
from exploration.drug_stats import save_pharmaceutical_stats, save_therapeutic_stats
from exploration.events_flowchart import flowchart_like_for_events
from exploration.exposures_stats import save_exposures_stats
from exploration.fall_study_stats import save_fractures_stats
from exploration.utils import get_spark_context


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("metapath", type=str, help="The filepath for the Metadata_json")
    parser.add_argument("flowpath", type=str, help="The filepath for the Flowchart_json")
    parser.add_argument("drugs_level", type=str, help="Enter Thera for Therapeutic study and Pharma "
                                                      "for Pharmaceutical study")
    parser.add_argument("molecule_mapping", type=str, help="Filepath for molecule_mapping_csv")
    parser.add_argument("output_path", type=str, help="Output directory")
    args = parser.parse_args()
    return args


def append_purchases_mapper(option, mapping, molecule_mapper) -> List[Tuple]:
    if option == "Pharma":
        mapping.append(("drug_purchases", functools.partial(
            save_pharmaceutical_stats,
            event_type="achats", colors_dict={
                "Antihypertenseurs": sns.color_palette()[0],
                "Antidepresseurs": sns.color_palette()[1],
                "Hypnotiques": sns.color_palette()[2],
                "Neuroleptiques": sns.color_palette()[3]
            },
            molecule_mapping_csv=molecule_mapper
        )
                        )
                       )

    else:
        mapping.append(("drug_purchases", functools.partial(
            save_therapeutic_stats,
            event_type="achats", colors_dict={
                "Antihypertenseurs": sns.color_palette()[0],
                "Antidepresseurs": sns.color_palette()[1],
                "Hypnotiques": sns.color_palette()[2],
                "Neuroleptiques": sns.color_palette()[3]
            }
        )
                        )
                       )
    return mapping


def append_exposures_mapper(option, mapping, molecule_mapper) -> List[Tuple]:
    if option == "Pharma":
        mapping.append(("exposures", functools.partial(
            save_pharmaceutical_stats,
            event_type="expositions", colors_dict={
                "Antihypertenseurs": sns.color_palette()[0],
                "Antidepresseurs": sns.color_palette()[1],
                "Hypnotiques": sns.color_palette()[2],
                "Neuroleptiques": sns.color_palette()[3]
            },
            molecule_mapping_csv=molecule_mapper
        )
                        )
                       )

    else:
        mapping.append(("exposures", functools.partial(
            save_therapeutic_stats,
            event_type="expositions", colors_dict={
                "Antihypertenseurs": sns.color_palette()[0],
                "Antidepresseurs": sns.color_palette()[1],
                "Hypnotiques": sns.color_palette()[2],
                "Neuroleptiques": sns.color_palette()[3]
            }
        )
                        )
                       )
    return mapping


def main():
    quiet_logs(get_spark_context())
    args = parse_arguments()
    with open(args.metapath, 'r') as fp:
        meta = json.load(fp)
    with open(args.flowpath, 'r') as fp:
        flowchart_json = json.load(fp)
    metadata_json = json.dumps(meta)
    mapping = [
        ("fractures", save_fractures_stats),
        ("exposures", save_exposures_stats)
    ]
    append_exposures_mapper(args.drugs_level, mapping, args.molecule_mapping)
    append_purchases_mapper(args.drugs_level, mapping, args.molecule_mapping)
    flowchart_like_for_events(flowchart_json, metadata_json, args.output_path, mapping)


if __name__ == "__main__":
    # execute only if run as a script
    main()
