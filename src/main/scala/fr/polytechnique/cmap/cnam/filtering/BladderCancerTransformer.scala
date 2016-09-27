package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

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
    col("start"),
    lit(null).cast(TimestampType).as("end")
  )

  val tmpOutputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("bladderCancer").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class ExtraDf(data: DataFrame) {

    def extractNarrowCancer: DataFrame = {
      data.filter(
        col("DP").startsWith(DiseaseCode) or
        col("DR").startsWith(DiseaseCode) or
        (col("DAS").startsWith(DiseaseCode) and col("DP").substr(0,3).isin(AssociateDiseases:_*)) or
        (col("DAS").startsWith(DiseaseCode) and col("DR").substr(0,3).isin(AssociateDiseases:_*))
      )
    }

    val window = Window.partitionBy(col("patientID")).orderBy(col("start"))

    def withDelta: DataFrame = {
      data
        .withColumn("nextTime", lead(col("start"), 1).over(window))
        .withColumn("nextDelta", months_between(col("nextTime"), coalesce(col("end"), col("start"))))
        .withColumn("previousDelta", lag(col("nextDelta"), 1).over(window))
    }

    def withNextType: DataFrame = {
      data
        .withColumn("nextType", lead(col("eventId"), 1).over(window))
        .withColumn("previousType", lag(col("eventId"),1).over(window))
    }

    def filterBladderCancer: DataFrame = {
      data.filter(col("eventID") === "bladderCancer")
        .filter((col("nextDelta") < 3 and col("nextType") === "radiotherapy") or
        (col("previousDelta") < 3 and col("previousType") === "radiotherapy"))
    }
  }

  override def transform(sources: Sources): Dataset[Event] = {
    import McoDiseaseTransformer.pmsiMcoDataFrame

    val mco = sources.pmsiMco.get
    import mco.sqlContext.implicits._

    val bladderCancers = mco
      .select(inputColumns:_*)
      .distinct()
      .extractNarrowCancer
      .estimateStayStartTime
      .select(tmpOutputColumns:_*)

    val radiotherapies = DcirActTransformer.transform(sources)

    val df = bladderCancers
      .unionAll(radiotherapies.toDF)


    df
      .withDelta
      .withNextType
      .filterBladderCancer
      .select(outputColumns:_*)
      .as[Event]
  }
}
