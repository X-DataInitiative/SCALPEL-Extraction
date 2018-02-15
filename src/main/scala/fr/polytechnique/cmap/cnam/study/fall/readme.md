# Fall Study

This study consists in searching for drug classes or molecules that can improve the risk of falling.

For achieving this, the input population contains all patients over 65 years old with their events (including drug purchases, hospitalizations, etc.) for 2015. As a proxy for a "fall", we use fractures.

## Pipeline Stages

### Source Reading

Consists in reading the flattened data from DCIR, MCO, `MCO_CE` and value tables such as `IR_IMB_R` and `IR_PHA_R` from parquet data into the cluster's memory.

### Patients Extraction

Stage responsible for reading `IR_BEN_R` for finding patients information. Additional data from MCO and DCIR is also used to complement missing information (e.g. death date).

### Patients Filter

We apply some filters required by the model to the population. The main one for now is to remove the patients who had drug purchase events within 2 months of the study start, with the intention of observing only "clean" patients.

### Drug Purchase Extraction

Finds CIP13 codes in DCIR to represent drug dispensations and classifies according to the required granularity (Therapeutical/Pharmaceutical classes or molecule-level).

### Exposures

Created from the drug purchases extracted previously. Several rules are applied depending on the definition required by the model. Usually we group all purchases that happen close to one another as a single exposure with the start date being the first purchase of the group and the end date being 30 days after the last purchase of the group. We also ensure no exposure starts within 30 days after the end of the previous exposure.

### Diagnoses Extraction

Search for all the diagnosis codes (mostly CIM10) that will be useful for our fracture definitions. Every useful code in the `DGN_PAL`, `DGN_REL` and `ASS_DGN` columns of MCO, as well as in the `IR_IMB_R` table, will be converted into a new Diagnosis line.

### Acts Extraction

In this stage, we search all the additional codes (CIM10, CCAM, GHM) that we will need for our outcomes. It includes every found code in the columns `CDC_ACT` and `DGN_PAL` in MCO, as well as acts from DCIR and `MCO_CE` tables, so it contains acts from multiple sources. The resulting data is used both for creating new fractures directly from acts, or for combining with diagnoses.

### Liberal Acts Extraction

A special case of Acts extraction responsible for selecting only acts from liberal sources (special CCAM codes from DCIR).

### Liberal Fractures

Creates the first fractures, which come directly from the liberal acts.

### Public Ambulatory Fractures

Creates outcomes from public ambulatories, which means selecting acts of particular CCAM codes from `MCO_CE`.

### Private Ambulatory Fractures

Similar to the public ones, but created from acts of particular CCAM codes from DCIR.

### Hospitalized Fractures

The main source of fractures. It takes the extracted diagnoses (DP and DAS) and acts (mostly GHM) as input and applies some rules for every hospitalization. When the rules apply, a fracture is created with the required granularity.
