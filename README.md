[![Build Status](https://travis-ci.com/X-DataInitiative/SNIIRAM-flattening.svg?token=LzAm1iAXuXZzFBCrak5F&branch=master)](https://travis-ci.com/X-DataInitiative/SNIIRAM-flattening)
[![codecov](https://codecov.io/gh/X-DataInitiative/SNIIRAM-flattening/branch/master/graph/badge.svg?token=4a0h501t8P)](https://codecov.io/gh/X-DataInitiative/SNIIRAM-flattening)

# Flattening

This repository hosts a cleaner version of flattening, including testing and statistics computation

The src/test/resources contains different fake csv including 2 different patients:
* Patient_01 is a female, who took GLICLAZIDE medicine and did not get cancer.
* Patient_02 is a male, he took PIOGLITAZONE and got cancer and died on 25/01/2006. But there is one purchase action for him in the PRS file.


To add a new table for the CNAM scope, we should modify the cnam.conf file.
It should be formatted like this : 

        cnam {
          tables = [
            {
            //here should come the description of one table
            }

            {
            // here should come another table
            }

          ]
        }
        
The object should be formatted like this :

    {
      name = IR_IMB_R
      paths = [
        "src/test/resources/IR_IMB_R.csv"
      ]
      fields = [
        {name: NUM_ENQ, type: StringType}
        {name: BEN_RNG_GEM, type: IntegerType}
        {name: IMB_ALD_DTD, type: DateType}
        {name: IMB_ALD_DTF, type: DateType}
        {name: IMB_ALD_NUM, type: IntegerType}
        {name: IMB_ETM_NAT, type: IntegerType}
        {name: IMB_MLP_BTR, type: StringType}
        {name: IMB_MLP_TAB, type: StringType}
        {name: IMB_SDR_LOP, type: StringType}
        {name: INS_DTE, type: DateType}
        {name: MED_MTF_COD, type: StringType}
        {name: MED_NCL_IDT, type: StringType}
        {name: UPD_DTE, type: DateType}
      ]
      output {
        table_name = "IR_IMB_R"
        key=2006
      }
    }
    
It is possible to add a "date_format" field that will describe how the date 
should be formatted following the java DateTime API.

# Filtering

The filtering package contains the ETL logic used to convert the flattened tables into two new tables: one containing the patients data and another one containing the normalized events as well as the outcomes. More details can be found in the [Software Architecture page](https://datainitiative.atlassian.net/wiki/display/CNAM/Software+architecture), on Confluence.

## FilteringMain

This package also contains a runnable object called `FilteringMain`. It can be run with spark-submit and it expects an "environment" parameter that can be either "test", "cnam" or "cmap", this will define which part of the `filtering.conf` configuration file will be used. Please make sure this file contains the correct paths before submitting the job. 

After a successful execution, the output tables will be written as parquet files in the paths defined in the `filtering.conf` file.

An example of execution of this class with the "test" environment is shown below:

```
$SPARK_HOME/bin/spark-submit \
   --class fr.polytechnique.cmap.cnam.filtering.FilteringMain \
   --master local[4] \
   this_package.jar test
```


