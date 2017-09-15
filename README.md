[![CircleCI](https://circleci.com/gh/X-DataInitiative/SNIIRAM-featuring.svg?style=shield&circle-token=6dd3a84c5ec9b9d3acac9e1ed156077eeadf9780)](https://circleci.com/gh/X-DataInitiative/SNIIRAM-featuring)
[![codecov](https://codecov.io/gh/X-DataInitiative/SNIIRAM-featuring/branch/master/graph/badge.svg?token=4a0h501t8P)](https://codecov.io/gh/X-DataInitiative/SNIIRAM-featuring)

This repository contains the ETL (Extract, Transform & Load) and Featuring stages of the processing pipeline for the SNIIRAM pharmacovigilance project in partnership with CNAMTS.

Please read our **[code conventions](https://datainitiative.atlassian.net/wiki/display/CFC/Development#)** before contributing to this repository.

# ETL stage

The ETL process of this project consists of the following steps:

1) Reading data from parquet files generated in the Flattening part;
2) Extracting events from raw data, such as diagnoses and drug purchases;
3) Transforming events into processed events, such as outcomes and exposures.

This processing stage can be executed by running the `ETLMain` class, which can be done with spark-shell as in the following example:

```bash
#!/bin/sh
spark-submit \
  --driver-memory 40G \
  --executor-memory 110G \
  --class fr.polytechnique.cmap.cnam.etl.ETLMain \
  --conf spark.task.maxFailures=20 \
  SNIIRAM-featuring-assembly-1.0.jar conf=./config.conf env=cnam
```

As shown above, the ETL step needs a configuration file. All the available configuration items can be found in the file [`src/main/resources/config/filtering-default.conf`](https://github.com/X-DataInitiative/SNIIRAM-featuring/blob/master/src/main/resources/config/filtering-default.conf).
However, the file passed at execution time does not need to contain all items, only those that different from default. Some examples are shown in the [Configuration](#configuration) section below.

# Featuring stage

The output of the ETL step is then passed into the Featuring step for converting specific events into formatted features ready to be used by a specific mathematical model.

Currently, the supported models are:

+ R's Coxph
+ Schuemie's LTSCCS
+ CMAP's AR-SCCS (a.k.a "MLPP")

For each model there is a main class that can be used to execute the featuring. These classes are listed below:

|Model|Class|
|:---|:---|
| Cox | `fr.polytechnique.cmap.cnam.etl.old_root.featuring.cox.CoxMain` |
| LTSCCS | `fr.polytechnique.cmap.cnam.etl.old_root.featuring.ltsccs.LTSCCSMain` |
| AR-SCCS | `fr.polytechnique.cmap.cnam.etl.old_root.featuring.mlpp.MLPPMain` |

For these classes, a configuration file is also needed and the format is the same as for the ETL code.

# Configuration

The configuration file passed at execution time can contain only the items that are different from the default. For example, to run only the ETL stage, but changing the list of diagnosis codes and the output paths, one could use a file such as the following:

```hocon
main_diagnosis_codes = ["I50", "I110", "130", "132"]

paths.output = {
    patients = "/new/output/path/patients"
    flat_events = "/new/output/path/new_events"
} 
```

Similarly when running the featuring for the AR-SCCS model, for example, we can change the number of lags and size of time buckets, in addition to the changes to the ETL configuration items:
 
```hocon
main_diagnosis_codes = ["I50", "I110", "130", "132"]

paths.output = {
    patients = "/new/output/path/patients"
    flat_events = "/new/output/path/new_events"
}

mlpp_parameters = {
    bucket_size = [60]  # Number of days of each bucket of time
    lag_count = [16]  # Number of lags to be created
}
```

All the available configuration items, with a quick description and the default values, can be found on the file
 [`src/main/resources/config/filtering-default.conf`](https://github.com/X-DataInitiative/SNIIRAM-featuring/blob/master/src/main/resources/config/filtering-default.conf). 
