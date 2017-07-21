package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object McoActTransformer extends Transformer[Event] {

  final val DiseaseCode = FilteringConfig.diseaseCode
  final val ActCodes = List("Z510", "Z511") // Chemotherapy / Radiotherapy
  final val AssociateDiseases = List("C77", "C78", "C79")

  // The two following sets are necessary due to the presence of the dot in their names.
  final val GHSColumns = Set[Column](
    col("MCO_B__GHM_24705Z_ACT"), col("MCO_B__GHM_24706Z_ACT"), col("MCO_B__GHM_24707Z_ACT"),
    col("MCO_B__GHS_6523_ACT"),
    col("MCO_B__GHS_9510_ACT"), col("MCO_B__GHS_9511_ACT"), col("MCO_B__GHS_9512_ACT"), col("MCO_B__GHS_9515_ACT"), col("MCO_B__GHS_9524_ACT"),
    col("MCO_B__GHS_9610_ACT"), col("MCO_B__GHS_9611_ACT"), col("MCO_B__GHS_9612_ACT"), col("MCO_B__GHS_9619_ACT"), col("MCO_B__GHS_9620_ACT"), col("MCO_B__GHS_9621_ACT")
  )

  final val GHSColumnNames = Set[String](
    "MCO_B__GHM_24705Z_ACT", "MCO_B__GHM_24706Z_ACT", "MCO_B__GHM_24707Z_ACT",
    "MCO_B__GHS_6523_ACT",
    "MCO_B__GHS_9510_ACT", "MCO_B__GHS_9511_ACT", "MCO_B__GHS_9512_ACT", "MCO_B__GHS_9515_ACT", "MCO_B__GHS_9524_ACT",
    "MCO_B__GHS_9610_ACT", "MCO_B__GHS_9611_ACT", "MCO_B__GHS_9612_ACT", "MCO_B__GHS_9619_ACT", "MCO_B__GHS_9620_ACT", "MCO_B__GHS_9621_ACT"
  )

  // The following list was taken from the following xls file:
  //    https://datainitiative.atlassian.net/wiki/download/attachments/40861700/Etude%20Pioglitazone%20-%20Crit%C3%A8re%20incidence%20cancer%20POUR%20ENVOI%20POLYTECHNIQUE%202016-09-19.xls?version=1&modificationDate=1474969426631&api=v2
  final val SpecificActs = List(
    "JDFA001", "JDFA003", "JDFA004", "JDFA005", "JDFA006", "JDFA008", "JDFA009", "JDFA011",
    "JDFA014", "JDFA015", "JDFA016", "JDFA017", "JDFA018", "JDFA019", "JDFA020", "JDFA021",
    "JDFA022", "JDFA023", "JDFA024", "JDFA025", "JDFC023", "JDLD002"
  )

  val inputColumns = List(
    col("NUM_ENQ").as("patientID"),
    col("MCO_B__DGN_PAL").as("DP"),
    col("MCO_B__DGN_REL").as("DR"),
    col("MCO_D__ASS_DGN").as("DAS"),
    col("MCO_A__CDC_ACT").as("CCAM"),
    col("MCO_B__SOR_MOI").as("stayMonthEndDate"),
    col("MCO_B__SOR_ANN").as("stayYearEndDate"),
    col("MCO_B__SEJ_NBJ").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  )

  val outputColumns = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("targetDisease").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  // Needed to avoid errors when a GHS column is not found. A warning is thrown if it's the case.
  def validGHSColumns(mco: DataFrame): Set[Column] =  {
    val result = GHSColumnNames.filter(mco.columns.contains(_))
    val diff = GHSColumnNames.diff(result)
    if (diff.nonEmpty)
      Logger.getLogger(getClass).warn(
        s"The columns in ${diff.toString} don't exist in the input MCO table. Ignoring them."
      )

    result.map(colName => col("`" + colName + "`"))
  }

  implicit class ActDF(data: DataFrame) {

    val ghsColumns = validGHSColumns(data)

    def filterBladderCancers: DataFrame = {
      val bladderCancerCondition =
        col("DP").startsWith(DiseaseCode) or
        col("DR").startsWith(DiseaseCode) or
        (col("DAS").startsWith(DiseaseCode) and col("DP").substr(0,3).isin(AssociateDiseases:_*)) or
        (col("DAS").startsWith(DiseaseCode) and col("DR").substr(0,3).isin(AssociateDiseases:_*))

      data.filter(bladderCancerCondition)
    }

    def filterActs: DataFrame = {
      data
        .filter(
          col("DP").substr(0, 4).isin(ActCodes: _*) or
          col("CCAM").substr(0, 7).isin(SpecificActs: _*) or
          ghsColumns.map(_ > 0).reduce(_ or _)
        )
    }
  }

  override def transform(sources: Sources): Dataset[Event] = {
    val mco = sources.pmsiMco.get

    import McoDiseaseTransformer.pmsiMcoDataFrame
    import mco.sqlContext.implicits._

    val bladderCancers = mco
      .select(inputColumns ++ validGHSColumns(mco): _*)
      .filterBladderCancers
      .estimateStayStartTime

    val targetCancers: Dataset[Event] = bladderCancers
      .filterActs
      .select(outputColumns: _*)
      .as[Event]

    bladderCancers
      .select(outputColumns: _*)
      .withColumn("eventId", lit("bladderCancer"))
      .as[Event]
      .union(targetCancers)
      .distinct
  }
}
