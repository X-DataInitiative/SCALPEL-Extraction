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
    * @param saveMode         The strategy of output data(default = overwrite)
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
    patientIdColName: String = "patientID"): OperationMetadata = {

    val dataPath: Path = Path(basePath, operationName, "data")
    val dataPathStr = dataPath.toString
    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType" to "$dataPathStr"""")

    val patientsPath: Path = Path(basePath, operationName, "patients")
    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType)

    outputType match {
      case OperationTypes.Patients =>
        data.writeParquet(dataPath.toString, saveMode)
        baseMetadata.copy(
          outputPath = dataPath.toString
        )

      case OperationTypes.Sources =>
        val patients = data.select(patientIdColName).distinct
        patients.writeParquet(patientsPath.toString, saveMode)
        baseMetadata.copy(
          populationPath = patientsPath.toString
        )

      case _ =>
        data.writeParquet(dataPath.toString, saveMode)
        val patients = data.select(patientIdColName).distinct
        patients.writeParquet(patientsPath.toString, saveMode)
        baseMetadata.copy(
          outputPath = dataPath.toString,
          populationPath = patientsPath.toString
        )
    }
  }

  def reportAsDataSet[A](
    operationName: String,
    operationInputs: List[String],
    outputType: OperationType,
    data: Dataset[A],
    basePath: Path,
    saveMode: String = "errorIfExists",
    patientIdColName: String = "patientID"): OperationMetadata = {

    val dataPath: Path = Path(basePath, operationName, "data")
    val dataPathStr = dataPath.toString
    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType" to "$dataPathStr"""")

    val patientsPath: Path = Path(basePath, operationName, "patients")
    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType)

    outputType match {
      case OperationTypes.Patients =>
        data.write.mode(saveModeFrom(saveMode)).parquet(dataPath.toString)
        baseMetadata.copy(
          outputPath = dataPath.toString
        )
      case _ =>
        data.write.mode(saveModeFrom(saveMode)).parquet(dataPath.toString)
        val patients = data.select(patientIdColName).distinct
        patients.write.mode(saveMode).parquet(patientsPath.toString)
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

  private def saveModeFrom(mode: String): SaveMode = mode match {
    case "overwrite" => SaveMode.Overwrite
    case "append" => SaveMode.Append
    case "errorIfExists" => SaveMode.ErrorIfExists
    case "withTimestamp" => SaveMode.Overwrite
  }
}
