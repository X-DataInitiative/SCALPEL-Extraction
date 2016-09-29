package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object McoActTransformer extends Transformer[Event] {

  // The two following sets are necessary due to the presence of the dot in their names.
  final val GHSColumns = Set[Column](
    col("`MCO_B.GHM_24705Z_ACT`"), col("`MCO_B.GHM_24706Z_ACT`"), col("`MCO_B.GHM_24707Z_ACT`"),
    col("`MCO_B.GHS_6523_ACT`"),
    col("`MCO_B.GHS_9510_ACT`"), col("`MCO_B.GHS_9511_ACT`"), col("`MCO_B.GHS_9512_ACT`"), col("`MCO_B.GHS_9515_ACT`"), col("`MCO_B.GHS_9524_ACT`"),
    col("`MCO_B.GHS_9610_ACT`"), col("`MCO_B.GHS_9611_ACT`"), col("`MCO_B.GHS_9612_ACT`"), col("`MCO_B.GHS_9619_ACT`"), col("`MCO_B.GHS_9620_ACT`"), col("`MCO_B.GHS_9621_ACT`")
  )

  final val GHSColumnNames = Set[String](
    "MCO_B.GHM_24705Z_ACT", "MCO_B.GHM_24706Z_ACT", "MCO_B.GHM_24707Z_ACT",
    "MCO_B.GHS_6523_ACT",
    "MCO_B.GHS_9510_ACT", "MCO_B.GHS_9511_ACT", "MCO_B.GHS_9512_ACT", "MCO_B.GHS_9515_ACT", "MCO_B.GHS_9524_ACT",
    "MCO_B.GHS_9610_ACT", "MCO_B.GHS_9611_ACT", "MCO_B.GHS_9612_ACT", "MCO_B.GHS_9619_ACT", "MCO_B.GHS_9620_ACT", "MCO_B.GHS_9621_ACT"
  )

  final val ActCodes = List("Z510", "Z511")
  final val AssociateDiseases = List("C77", "C78", "C79")

  // The following list was taken from the following xls file:
  //    https://datainitiative.atlassian.net/wiki/download/attachments/40861700/Etude%20Pioglitazone%20-%20Crit%C3%A8re%20incidence%20cancer%20POUR%20ENVOI%20POLYTECHNIQUE%202016-09-19.xls?version=1&modificationDate=1474969426631&api=v2
  final val SpecificActs = List(
    "JDFA001", "JDFA003", "JDFA004", "JDFA005", "JDFA006", "JDFA008", "JDFA009", "JDFA011",
    "JDFA014", "JDFA015", "JDFA016", "JDFA017", "JDFA018", "JDFA019", "JDFA020", "JDFA021",
    "JDFA022", "JDFA023", "JDFA024", "JDFA025", "JDFC023", "JDLD002"
  )

  val inputColumns = List(
    col("NUM_ENQ").as("patientID"),
    col("`MCO_B.DGN_PAL`").as("DP"),
    col("`MCO_B.DGN_REL`").as("DR"),
    col("`MCO_D.ASS_DGN`").as("DAS"),
    col("`MCO_A.CDC_ACT`").as("CCAM"),
    col("`MCO_B.SOR_MOI`").as("stayMonthEndDate"),
    col("`MCO_B.SOR_ANN`").as("stayYearEndDate"),
    col("`MCO_B.SEJ_NBJ`").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  ) ++ GHSColumns

  val outputColumns = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("targetDisease").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class ActDF(data: DataFrame) {

    def filterActs: DataFrame = {
      import BladderCancerTransformer.bladderCancerCondition
      data
        .filter(
          bladderCancerCondition and (
            col("DP").substr(0, 4).isin(ActCodes: _*) or
            col("CCAM").substr(0, 7).isin(SpecificActs: _*) or
            GHSColumns.map(_ > 0).reduce(_ or _)
          )
        )
    }
  }

  override def transform(sources: Sources): Dataset[Event] = {
    val mco = sources.pmsiMco.get

    import McoDiseaseTransformer.pmsiMcoDataFrame
    import mco.sqlContext.implicits._

    mco
      .select(inputColumns: _*)
      .filterActs
      .estimateStayStartTime
      .select(outputColumns: _*)
      .as[Event]
      .distinct
  }
}
