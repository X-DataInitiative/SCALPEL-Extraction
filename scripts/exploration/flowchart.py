import json
from collections import namedtuple
from copy import deepcopy
from typing import Dict, List, NamedTuple, Optional
from dateutil.parser import parse
from os import path

from pyspark.sql import DataFrame
import pandas as pd

from .patients_stats import do_plot_stats, do_show_stats, save_patients_stats
from .utils import read_data_frame

#################################
#### Metadata and Operation  ####
#################################


Metadata = namedtuple(
    'Metadata',
    ['name', 'params', 'start', 'end', 'operations']
)
Operation = namedtuple(
    'Operation',
    ['name', 'inputs', 'outputPath', 'patients_output']
)


def parse_operations(operations_list: List[Dict]) -> Dict[str, Operation]:
    operations = {}
    for operation in operations_list:
        output_path = operation['output_path'] if 'output_path' in operation else None
        population_path = (
            operation['population_path'] if operation['output_type'] != 'patients'
            else operation['output_path']
        )
        operations[operation['name']] = Operation(operation['name'], operation['inputs'],
                                                  output_path, population_path)

    return operations


def parse_metadata(content: str) -> Metadata:
    dumped_dict = json.loads(content)

    operations = dumped_dict.get('operations')

    return Metadata(
        dumped_dict['class_name'],
        None,
        parse(dumped_dict['start_timestamp']),
        parse(dumped_dict['end_timestamp']),
        parse_operations(operations)
    )


#################################
#### Cohort Manipulations    ####
#################################


Cohort = NamedTuple(
    "Cohort", [('name', str), ("parents", List[str]), ('patients', DataFrame)])


def construct_name(operation: Operation) -> str:
    return "{}".format(operation.name)


def read_patients(operation: Operation) -> DataFrame:
    patients_df = read_data_frame(operation.patients_output)
    return patients_df


def get_parents(operation: Operation) -> List[str]:
    return deepcopy(operation.inputs)


def extract_cohort(operation: Operation) -> Cohort:
    print("Constructing cohort for operation '{}'".format(operation.name))
    cohort_name = construct_name(operation)
    print("Reading patients")

    patients = read_patients(operation)
    print("Finished Reading")
    parents = get_parents(operation)
    return Cohort(cohort_name, parents, patients)


def map_metadata_to_cohorts(metadata: Metadata) -> Dict[str, Cohort]:
    return {
        cohort.name: cohort
        for cohort in
        [extract_cohort(operation) for _, operation in metadata.operations.items()]
    }


#################################
####      Flow chart         ####
#################################


def get_patients_information(patients: DataFrame, base_patients: DataFrame) -> DataFrame:
    # TODO: Should be a LEFT join
    return patients.join(base_patients, on=['patientID'], how='inner')


def get_filtered_patients(cohort: Cohort,
                          cohorts: Dict[str, Cohort]) -> Optional[DataFrame]:
    parents = [cohorts[name] for name in cohort.parents if name != 'source']

    try:
        assert len(parents) > 0
        union = parents[0].patients.select("patientID")

        for parent in parents[1:]:
            union = union.union(parent.patients.select("patientID")).dropDuplicates(
                ["patientID"])

        filtered_patients = union.subtract(cohort.patients.select("patientID")).cache()

        return filtered_patients
    except AssertionError:
        if cohort.name == 'source':
            pass
        else:
            print("Cohort {} has no parents".format(cohort.name))


def report_flow_chart(metadata: Metadata, base_population_name) -> None:
    cohorts = map_metadata_to_cohorts(metadata)
    base_population = cohorts[base_population_name].patients

    for (name, cohort) in cohorts.items():
        filtered_patients_id = get_filtered_patients(cohort, cohorts)
        if filtered_patients_id is None:
            print("DrugPurchaseStatsPlotter for Operation {} is not available for the moment".format(name))
        else:
            size = filtered_patients_id.count()
            print("Plotting for {} filtered cohort. Size {}.".format(name, size))
            if size > 0:
                filtered_patients = get_patients_information(filtered_patients_id,
                                                             base_population)
                do_plot_stats(filtered_patients, "Filtered after {}".format(name))


def cohort_union(parents: List[Cohort], result_cohort_name: str) -> Cohort:
    parents_name = [parent.name for parent in parents]

    union = parents[0].patients

    for other in parents[1:]:
        union = union.union(other.patients).dropDuplicates()

    return Cohort(result_cohort_name, parents_name, union)


def cohort_intersection(cohorts: List[Cohort], result_cohort_name: str) -> Cohort:
    parents = [parent.name for parent in cohorts]

    intersect = cohorts[0].patients

    for other in cohorts[1:]:
        intersect = intersect.join(other.patients.select("patientID"),
                                   on=["patientID"], how="inner")

    return Cohort(result_cohort_name, parents, intersect)


def intermediate_cohorts_builder(cohorts: Dict[str, Cohort],
                                 intermediate_cohorts: Dict) -> Dict[str, Cohort]:
    result = dict()
    for cohort_name, parents_name in intermediate_cohorts.items():
        parents = [cohorts[parent_name] for parent_name in parents_name]
        result[cohort_name] = cohort_union(parents, cohort_name)

    return result


def steps_builder(cohorts: Dict[str, Cohort],
                  steps_cohorts: List[str]) -> Dict[str, Cohort]:
    result = dict()
    # The first passed cohort is special, it doesnt need any intersection
    result[0] = cohorts[steps_cohorts[0]]

    # The remaining should be intersected for to form the steps
    for i, cohort_name in enumerate(steps_cohorts[1:]):
        name = "Step_{}".format(i+1)
        result[i+1] = cohort_intersection([result[i], cohorts[cohort_name]], name)
    return result


def flowchart_file_parser(s: str, cohorts: Dict[str, Cohort]) -> Dict[str, Cohort]:
    flowchart_description = json.loads(s)
    intermediate_cohorts = intermediate_cohorts_builder(
        cohorts, flowchart_description["intermediate_cohorts"])
    updated_cohorts = {**intermediate_cohorts, **cohorts}
    steps = steps_builder(updated_cohorts, flowchart_description["steps"])

    return steps


def report_counts(steps: Dict[str, Cohort], root_directory: str) -> None:
    counts = []
    steps_enum = []

    for (i, cohort) in steps.items():
        counts.append(cohort.patients.count())
        steps_enum.append(i)

    count_df = pd.DataFrame.from_dict({"step_number": steps_enum,
                                       "population_count": counts})
    output_path = path.join(root_directory, "counts.csv")
    print("Writing down counts to {}".format(output_path))
    count_df.to_csv(output_path)


def on_demand_flowchart(metadata_json: str, flowchart_json: str, root_directory: str,
                        plot_percentage=False):
    metadata = parse_metadata(metadata_json)
    cohorts = map_metadata_to_cohorts(metadata)
    steps = flowchart_file_parser(flowchart_json, cohorts)

    for (i, cohort) in steps.items():
        total = cohort.patients.count() if plot_percentage else None
        save_patients_stats(cohort.patients, "Population apres l'etape {}".format(i),
                            total, root_directory)

    report_counts(steps, root_directory)


def flowchart(metadata_json: str, base_population_name='extract_patients') -> None:
    metadata = parse_metadata(metadata_json)
    report_flow_chart(metadata, base_population_name)
