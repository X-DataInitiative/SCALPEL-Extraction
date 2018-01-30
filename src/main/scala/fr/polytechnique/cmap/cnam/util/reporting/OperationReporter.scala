package fr.polytechnique.cmap.cnam.util.reporting

import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.util.Path
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

/**
  * Singleton responsible for reporting an operation execution.
  * Includes three main actions:
  *   1) writing the operation output data,
  *   2) writing the distinct patients present in the output data and
  *   3) computing counts for both datasets
  *
  * Note: An operation can be any method that touches a DataFrame, including but not limited to: readers,
  *   extractors, transformers and filters.
  */
object OperationReporter {

  private val logger = Logger.getLogger(this.getClass)

  private def writeCsv(data: DataFrame, path: Path): Unit = {
    data.write.mode(SaveMode.Overwrite).option("header", "true").csv(path.toString)
  }

  private def writeParquet(data: DataFrame, path: Path): Unit = {
    data.write.mode(SaveMode.Overwrite).parquet(path.toString)
  }

  /**
    * The main method for generating the report for the given operation
    * @param operationName The unique name (ex: "diagnoses")
    * @param operationInputs The unique names of the previous operations on which this one depends
    * @param data The output data (ex: diagnoses)
    * @param basePath The base path where the data and patients will be written
    * @param patientsLookup (Optional, default=None) A full Dataset of patients for lookup of age and gender
    * @param patientIdColName (default="patientID") The column name of the patientID in the output data
    * @return an instance of OperationMetadata
    */
  def report(
      operationName: String,
      operationInputs: List[String],
      data: DataFrame,
      basePath: Path,
      patientsLookup: Option[Dataset[Patient]] = None,
      patientIdColName: String = "patientID"): OperationMetadata = {

    val operationPath: Path = Path(basePath, operationName)

    // write data and compute count
    logger.info(s"=> Reporting operation $operationName")
    logger.info(s"     Persisting and counting data...")
    data.persist()
    val dataCount = data.count
    logger.info(s"     Count: $dataCount")
    val dataPath = Path(operationPath, "data")
    logger.info(s"     Writing data to path ${dataPath.toString}")
    writeParquet(data, dataPath)

    patientsLookup match {
      // When we have a patientsLookup dataset, we get the distinct patients and write the population
      case Some(lookup) => {
        logger.info(s"  Persisting and counting patients for operation $operationName...")
        val patients = data.select(data(patientIdColName).as("patientID")).join(lookup, "patientID").distinct()
        patients.persist()
        val patientsCount = patients.count.toInt
        logger.info(s"  Count: $patientsCount")
        val patientsPath = Path(operationPath, "patients")
        logger.info(s"     Writing data to path ${patientsPath.toString}")
        writeParquet(patients, patientsPath)
        OperationMetadata(
          operationName, operationInputs,
          dataPath.toString, dataCount,
          Some(patientsPath.toString), Some(patientsCount)
        )
      }

      // Otherwise, we don't need to compute the patients and write them
      case None => OperationMetadata(
        operationName, operationInputs,
        dataPath.toString, dataCount,
        None, None
      )
    }
  }
}
