package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object BladderCancerTransformer extends TargetDiseaseTransformer {

  final val AssociateDiseases = List("C77", "C78", "C79")

  val inputColumns = List(
    col("NUM_ENQ").as("patientID"),
    col("`MCO_B.DGN_PAL`").as("DP"),
    col("`MCO_B.DGN_REL`").as("DR"),
    col("`MCO_D.ASS_DGN`").as("DAS"),
    col("`MCO_B.SOR_MOI`").as("stayMonthEndDate"),
    col("`MCO_B.SOR_ANN`").as("stayYearEndDate"),
    col("`MCO_B.SEJ_NBJ`").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  )

  override val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("bladderCancer").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  val bladderCancerCondition =
    col("DP").startsWith(DiseaseCode) or
    col("DR").startsWith(DiseaseCode) or
    (col("DAS").startsWith(DiseaseCode) and col("DP").substr(0,3).isin(AssociateDiseases:_*)) or
    (col("DAS").startsWith(DiseaseCode) and col("DR").substr(0,3).isin(AssociateDiseases:_*))

  implicit class BladderCancerDataFrame(data: DataFrame) {
    def extractBladderCancer: DataFrame = data.filter(bladderCancerCondition)
  }

  override def transform(sources: Sources): Dataset[Event] = {
    import McoDiseaseTransformer.pmsiMcoDataFrame

    val mco = sources.pmsiMco.get
    import mco.sqlContext.implicits._

    mco.select(inputColumns:_*)
      .distinct()
      .extractBladderCancer
      .estimateStayStartTime
      .select(outputColumns:_*)
      .as[Event]
  }
}
