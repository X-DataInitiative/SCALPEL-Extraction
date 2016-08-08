# flattening
[![Build Status](https://travis-ci.com/X-DataInitiative/SNIIRAM-flattening.svg?token=LzAm1iAXuXZzFBCrak5F&branch=master)](https://travis-ci.com/X-DataInitiative/SNIIRAM-flattening)

This repository host a cleaner version of flattening, including testing and statistics computation

The src/test/resources contains different fake csv including 2 different patients:
* Patient_01 is a female, who took GLICLAZIDE medicine and didn't get cancer.
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

