// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp
import scala.reflect.runtime.universe
import org.apache.commons.codec.binary.Base64
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


  /** It generate a hash using the values of these columns
    *(FLX_DIS_DTD,FLX_TRT_DTD,FLX_EMT_TYP,FLX_EMT_NUM,FLX_EMT_ORD,ORG_CLE_NUM,DCT_ORD_NUM).
    * It allows to identify each prescription in a unique way, it can be used to identify
    * the possible interactions of molecules prescript in the same period.
    *
    * @param r The Row object itself
    * @return A hash Id unique in a string format
    */
  def extractGroupId(r: Row): String = {
    Base64.encodeBase64(
      s"${r.getAs[String](ColNames.FluxDate)}_${r.getAs[String](ColNames.FluxProcessingDate)}_${
        r.getAs[String](
          ColNames
            .EmitterType
        )
      }_${r.getAs[String](ColNames.EmitterId)}_${r.getAs[String](ColNames.FluxSeqNumber)}_${
        r.getAs[String](
          ColNames
            .OrganisationOldId
        )
      }_${r.getAs[String](ColNames.OrganisationDecompteNumber)}".getBytes()
    ).map(_.toChar).mkString
  }


  override def isInStudy(codes: Set[String])
    (row: Row): Boolean = drugConfig.level.isInFamily(drugConfig.families, row)

  override def isInExtractorScope(row: Row): Boolean = true

  override def builder(row: Row): Seq[Event[Drug]] = {
    lazy val classification = drugConfig.level.getClassification(drugConfig.families)(row)

    lazy val patientID = getPatientID(row)
    lazy val conditioning = getConditioning(row)
    lazy val date = getEventDate(row)
    lazy val groupID = extractGroupId(row)

    classification.map(code => Drug(patientID, code, conditioning, groupID, date))
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
      col("molecule_combination").cast(StringType).as("molecules"),
      col("PHA_CND_TOP").cast(StringType).as("conditioning")
    ) ::: ColNames.GroupID.map(col)

    lazy val irPhaR = sources.irPha.get
    lazy val dcir = sources.dcir.get
    val spark: SparkSession = dcir.sparkSession

    lazy val df: DataFrame = dcir.join(irPhaR, dcir.col("ER_PHA_F__PHA_PRS_C13") === irPhaR.col("PHA_CIP_C13"))

    df
      .select(neededColumns: _*)
      .withColumn("conditioning", when(col("conditioning") === "GC", 1).otherwise(2))
      .na.drop(Seq("eventDate", "CIP13", "ATC5"))
  }

  final object ColNames extends Serializable {
    val PatientId = "patientID"
    val Conditioning = "conditioning"
    val Date = "eventDate"
    val Cip13 = "CIP13"

    lazy val FluxDate = "FLX_DIS_DTD"
    lazy val FluxProcessingDate = "FLX_TRT_DTD"
    lazy val EmitterType = "FLX_EMT_TYP"
    lazy val EmitterId = "FLX_EMT_NUM"
    lazy val FluxSeqNumber = "FLX_EMT_ORD"
    lazy val OrganisationOldId = "ORG_CLE_NUM"
    lazy val OrganisationDecompteNumber = "DCT_ORD_NUM"

    lazy val GroupID = List(
      FluxDate, FluxProcessingDate, EmitterType, EmitterId, FluxSeqNumber, OrganisationOldId, OrganisationDecompteNumber
    )
  }

}
