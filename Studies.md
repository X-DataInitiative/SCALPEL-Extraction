# Studies

A study is created using [Extractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/Extractor.scala),
[Transformer](Transformer.md), [Events](Events.md), 
[Configuration](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/study/fall/config/FallConfig.scala) 
and a [main](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/study/fall/FallMain.scala) 
element that manages the treatment.
To exemplify this process we will use the  [Fall](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/study/fall/)'s study.

The entry point for this study is the [FallMain](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/study/fall/FallMain.scala) 
object inherited from [Main](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/Main.scala) 
and [FractureCodes](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/study/fall/codes/FractureCodes.scala), 
from the first one you get the necessary elements to create a Spark session, use it and destroy it; from the second one 
you get the codes to identify fractures.

In the `Main` object we find the `main` method that contains the necessary to start and stop SparkSession,
recover the arguments that are going to be 
used during the study, these arguments are the path of the configuration file and the 
environment in which the treatment is launched and to pass both of them as parameters through `run` method.

```
 def main(args: Array[String]): Unit = {
   //This method starts the SparkSession
    startContext()
    val sqlCtx = sqlContext
    val argsMap = args.map(
      arg => arg.split("=")(0) -> arg.split("=")(1)
    ).toMap
    try {
   //This method allows pass as parameters SQLContext and parsed arguments
      run(sqlCtx, argsMap)
    }
    finally stopContext()
  }
```
```
Arguments

 "conf"="/src/main/resources/config/fall/default.conf","env"="test"
```

The first step once treatment is running is load in config object all config values from config file.

The `FallConfig` object allows to parametrize the study, with input, output and other parameters
to control study's values. There are a template to use with fall study.

```
# Template configuration file for the Fall study. To override the defaults, copy this file to your working
#   directory, then uncomment the desired lines and pass the file path to spark-submit

# input.dcir = "src/test/resources/test-input/DCIR.parquet"
# input.mco = "src/test/resources/test-input/MCO.parquet"
# input.mco_ce = "src/test/resources/test-input/MCO_CE.parquet"
# input.ir_ben = "src/test/resources/test-input/IR_BEN_R.parquet"
# input.ir_imb = "src/test/resources/test-input/IR_IMB_R.parquet"
# input.ir_pha = "src/test/resources/test-input/IR_PHA_R_With_molecules.parquet"

# output.root = "target/test/output"
# output.save_mode = "errorIfExists"   // Possible values = [overwrite, append, errorIfExists, withTimestamp] Strategy of saving output data. errorIfExists by deault

# exposures.start_delay: 0 months      // 0+ (Usually between 0 and 3). Represents the delay in months between a dispensation and its exposure start date.
# exposures.purchases_window: 0 months // 0+ (Usually 0 or 6) Represents the window size in months. Ignored when min_purchases=1.
# exposures.end_threshold_gc: 90 days  // If periodStrategy="limited", represents the period without purchases for an exposure to be considered "finished".
# exposures.end_threshold_ngc: 30 days // If periodStrategy="limited", represents the period without purchases for an exposure to be considered "finished".
# exposures.end_delay: 30 days         // Number of periods that we add to the exposure end to delay it (lag).

# interactions.level: 3 // Integer representing the maximum number of values of Interaction. Please be careful as this not scale well beyond 5 when the data contains a patient with very high number of exposures

# drugs.level: "Therapeutic"           // Options are Therapeutic, Pharmacological, MoleculeCombination
# drugs.families: ["Antihypertenseurs", "Antidepresseurs", "Neuroleptiques", "Hypnotiques"]

# patients.start_gap_in_months: 2      // filter Removes all patients who have got an event within N months after the study start.

# sites.sites: ["BodySites"]

# outcomes.fall_frame: 0 months        // fractures are grouped if they happen in the same site within the period fallFrame, (default value 0 means no group)

# run_parameters.outcome: ["Acts", "Diagnoses", "Outcomes"]                                           // pipeline of calculation of outcome, possible values : Acts, Diagnoses, and Outcomes
# run_parameters.exposure: ["Patients", "StartGapPatients", "DrugPurchases", "Exposures"]             // pipeline of the calculation of exposure, possible values : Patients, StartGapPatients, DrugPurchases, Exposures

```

extracting from config object  paths to load sources in sources objects or parameters to filter 
study's data.
Later it runs three methods that yield [OperationMetadata](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/util/reporting/OperationMetadata.scala).

All three methods takes the same parameters `Sources` and `FallConfig`, they are:

- computeControls
- computeExposures
- computeOutcomes

Here as example the output of `computeExposures`:

```
OperationMetadata(drug_purchases,List(DCIR),dispensations,target/test/output/drug_purchases/data,target/test/output/drug_purchases/patients), 
OperationMetadata(extract_patients,List(DCIR, MCO, IR_BEN_R, MCO_CE),patients,target/test/output/extract_patients/data,), 
OperationMetadata(filter_patients,List(drug_purchases, extract_patients),patients,target/test/output/filter_patients/data,)
```
The result of these methods, all `OperationMetadata` are stored in a value `operationsMetadata`
and this values is stored with other descriptive values 
(class name,start timestamp,end timestamp,operationsMetadata) in a case class `MainMetadata`.

```
case class MainMetadata(
  className: String,
  startTimestamp: java.util.Date,
  endTimestamp: java.util.Date,
  operations: List[OperationMetadata])
  extends JsonSerializable
```

This is the result of the study, is saved as Json in a environment and path passed as parameters to
`writeMetaData` method.