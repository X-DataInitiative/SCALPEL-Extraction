// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.reporting

import java.io.PrintWriter
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import fr.polytechnique.cmap.cnam.util.Path
import fr.polytechnique.cmap.cnam.util.RichDataFrame._

/**
  * Singleton responsible for reporting an operation execution.
  * Includes three main actions:
  * 1) writing the operation output data,
  * 2) writing the distinct patients present in the output data and
  * 3) computing counts for both datasets
  *
  * Note: An operation can be any method that touches a DataFrame, including but not limited to: readers,
  * extractors, transformers and filters.
  */
object OperationReporter {

  private val logger = Logger.getLogger(this.getClass)

  /**
    * The main method for generating the report for the given operation
    *
    * @param operationName    The unique name (ex: "diagnoses")
    * @param operationInputs  The unique names of the previous operations on which this one depends
    * @param outputType       The type of the operation output
    * @param data             The output data (ex: diagnoses)
    * @param basePath         The base path where the data and patients will be written
    * @param saveMode         The strategy of output data(default = errorIfExists)
    * @param patientIdColName (default="patientID") The column name of the patientID in the output data
    * @return an instance of OperationMetadata
    */
  def report(
    operationName: String,
    operationInputs: List[String],
    outputType: OperationType,
    data: DataFrame,
    basePath: Path,
    saveMode: String = "errorIfExists",
    format: String = "parquet",
    patientIdColName: String = "patientID"): OperationMetadata = {

    val dataPath: Path = Path(basePath, operationName, "data")
    val dataPathStr = dataPath.toString
    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType" to "$dataPathStr"""")

    val patientsPath: Path = Path(basePath, operationName, "patients")
    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType)

    outputType match {
      case OperationTypes.Patients =>
        data.write(dataPath.toString, saveMode, format)
        baseMetadata.copy(
          outputPath = dataPath.toString
        )

      case OperationTypes.Sources =>
        val patients = data.select(patientIdColName).distinct
        patients.write(patientsPath.toString, saveMode, format)
        baseMetadata.copy(
          populationPath = patientsPath.toString
        )

      case _ =>
        data.write(dataPath.toString, saveMode, format)
        val patients = data.select(patientIdColName).distinct
        patients.write(patientsPath.toString, saveMode, format)
        baseMetadata.copy(
          outputPath = dataPath.toString,
          populationPath = patientsPath.toString
        )
    }
  }

  /**
    * The main method for generating the report for the given operation
    *
    * @param operationName The unique name (ex: "diagnoses")
    * @param operationInputs The unique names of the previous operations on which this one depends
    * @param outputType The type of the operation output
    * @param data The output data (ex: diagnoses)
    * @param basePath The base path where the data and patients will be written
    * @param saveMode The strategy of output data(default = errorIfExists)
    * @param format The format to save file(default = parquet)
    * @param patientIdColName (default="patientID") The column name of the patientID in the output data
    * @tparam A
    * @return an instance of OperationMetadata
    */
  def reportAsDataSet[A](
    operationName: String,
    operationInputs: List[String],
    outputType: OperationType,
    data: Dataset[A],
    basePath: Path,
    saveMode: String = "errorIfExists",
    format: String = "parquet",
    patientIdColName: String = "patientID"): OperationMetadata = {

    val dataPath: Path = Path(basePath, operationName, "data")
    val dataPathStr = dataPath.toString
    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType" to "$dataPathStr"""")

    val patientsPath: Path = Path(basePath, operationName, "patients")
    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType)

    outputType match {
      case OperationTypes.Patients =>
        writeDataSet(data, dataPath.toString, saveMode, format)
        baseMetadata.copy(
          outputPath = dataPath.toString
        )
      case _ =>
        writeDataSet(data, dataPath.toString, saveMode, format)
        val patients = data.select(patientIdColName).distinct
        writeDataSet(patients, patientsPath.toString, saveMode, format)
        baseMetadata.copy(
          outputPath = dataPath.toString,
          populationPath = patientsPath.toString
        )
    }
  }

  /**
    * The main method for generating the report for the given operation
    *
    * @param operationName The unique name (ex: "diagnoses")
    * @param operationInputs The unique names of the previous operations on which this one depends
    * @param outputType The type of the operation output
    * @param data The output data (ex: diagnoses)
    * @param basePath The base path where the data and patients will be written
    * @param saveMode The strategy of output data(default = errorIfExists)
    * @param format The format to save file(default = parquet)
    * @tparam A
    * @tparam B
    * @return an instance of OperationMetadata
    */
  def reportDataAndPopulationAsDataSet[A, B](
    operationName: String,
    operationInputs: List[String],
    outputType: OperationType,
    data: Dataset[A],
    population: Dataset[B],
    basePath: Path,
    saveMode: String = "errorIfExists",
    format: String = "parquet"): OperationMetadata = {
    val dataPath: Path = Path(basePath, operationName, "data")
    val dataPathStr = dataPath.toString
    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType" to "$dataPathStr"""")

    val patientsPath: Path = Path(basePath, operationName, "patients")
    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType)

    writeDataSet(data, dataPath.toString, saveMode, format)
    outputType match {
      case OperationTypes.Patients =>
        baseMetadata.copy(
          outputPath = dataPath.toString
        )
      case _ =>
        writeDataSet(population, patientsPath.toString, saveMode, format)
        baseMetadata.copy(
          outputPath = dataPath.toString,
          populationPath = patientsPath.toString
        )
    }
  }

  /**
    * the method to output the meta data
    *
    * @param metaData meta data
    * @param path     storage path of meta data
    * @param env      running environment
    */
  def writeMetaData(metaData: String, path: String, env: String): Unit = {
    if (env != "test") {
      new PrintWriter(path) {
        write(metaData)
        close()
      }
    }
  }

  private def writeDataSet[A](data: Dataset[A], path: String, mode: String, format: String = "parquet") = {
    format match {
      case "orc" => data.write.mode(saveModeFrom(mode)).orc(path)
      case "csv" =>
        data
          .coalesce(1)
          .write
          .mode(saveModeFrom(mode))
          .option("delimiter", ",")
          .option("header", "true")
          .csv(path)
      case _ => data.write.mode(saveModeFrom(mode)).parquet(path)
    }
  }

  private def saveModeFrom(mode: String): SaveMode = mode match {
    case "overwrite" => SaveMode.Overwrite
    case "append" => SaveMode.Append
    case "errorIfExists" => SaveMode.ErrorIfExists
    case "withTimestamp" => SaveMode.Overwrite
  }
}
