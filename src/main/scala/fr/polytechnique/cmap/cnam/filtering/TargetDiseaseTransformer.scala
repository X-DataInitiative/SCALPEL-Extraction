package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, DataFrame, Dataset}

trait TargetDiseaseTransformer extends Transformer[Event] {

  final val DiseaseCode = FilteringConfig.diseaseCode

  val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("targetDisease").as("eventId"),
    lit(1.00).as("weight"),
    col("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class TargetDiseaseDataFrame(data: DataFrame) {

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

    def filterDcirTargetDiseases: DataFrame = {
      data
        .filter(col("eventId") === "bladderCancer")
        .filter(
          (col("nextDelta") < 3 and col("nextType") === "radiotherapy") or
          (col("previousDelta") < 3 and col("previousType") === "radiotherapy")
        )
    }
  }
}

object TargetDiseaseTransformer extends TargetDiseaseTransformer {
  override def transform(sources: Sources): Dataset[Event] = {

    val mcoCancers: Dataset[Event] = McoActTransformer.transform(sources)
    import mcoCancers.sqlContext.implicits._

    val bladderCancers: Dataset[Event] = mcoCancers.filter(_.eventId == "bladderCancer")
    val mcoTargetDiseases: Dataset[Event] = mcoCancers.filter(_.eventId == "targetDisease")

    val dcirTargetDiseases = bladderCancers
      .union(DcirActTransformer.transform(sources))
      .toDF
      .withDelta
      .withNextType
      .filterDcirTargetDiseases
      .select(outputColumns: _*)
      .as[Event]

    dcirTargetDiseases.union(mcoTargetDiseases).distinct
  }
}
