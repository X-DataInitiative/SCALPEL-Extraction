package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp
import scala.reflect.runtime.universe
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, TimestampType}
import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class DrugExtractor(drugConfig: DrugConfig) extends Extractor[Drug] {

  override def extract(
    sources: Sources,
    codes: Set[String])
    (implicit ctag: universe.TypeTag[Drug]): Dataset[Event[Drug]] = {

    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (drugConfig.families.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
    }.flatMap(builder _).distinct()

  }

  override def isInStudy(codes: Set[String])
    (row: Row): Boolean = drugConfig.level.isInFamily(drugConfig.families, row)

  override def isInExtractorScope(row: Row): Boolean = true

  override def builder(row: Row): Seq[Event[Drug]] = {
    lazy val classification = drugConfig.level.getClassification(drugConfig.families)(row)

    lazy val patientID = getPatientID(row)
    lazy val conditioning = getConditioning(row)
    lazy val date = getEventDate(row)

    classification.map(code => Drug(patientID, code, conditioning, date))
  }

  private def getPatientID(row: Row): String = row.getAs[String](ColNames.PatientId)

  private def getConditioning(row: Row): Int = row.getAs[Int](ColNames.Conditioning)

  private def getEventDate(row: Row): Timestamp = row.getAs[Timestamp](ColNames.Date)

  override def getInput(sources: Sources): DataFrame = {
    val neededColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("PHA_ATC_C07").cast(StringType).as("ATC5"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate"),
      //col("molecule_combination").cast(StringType).as("molecules"),
      col("PHA_CND_TOP").cast(StringType).as("conditioning")
    )

    lazy val irPhaR = sources.irPha.get
    lazy val dcir = sources.dcir.get
    val spark: SparkSession = dcir.sparkSession

    lazy val df: DataFrame = dcir.join(irPhaR, dcir.col("ER_PHA_F__PHA_PRS_C13") === irPhaR.col("PHA_CIP_C13"))

    df
      .select(neededColumns: _*)
      .withColumn("conditioning", when(col("conditioning") === "GC", 1).otherwise(2))
      .na.drop(Seq("eventDate", "CIP13", "ATC5"))
  }

  final object ColNames {
    val PatientId = "patientID"
    val Conditioning = "conditioning"
    val Date = "eventDate"
    val Cip13 = "CIP13"
  }

}
