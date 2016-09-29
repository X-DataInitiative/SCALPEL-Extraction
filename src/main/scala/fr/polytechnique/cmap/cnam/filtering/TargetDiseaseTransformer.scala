package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

trait TargetDiseaseTransformer extends Transformer[Event] {

  final val DiseaseCode = "C67"

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

    def filterBladderCancer: DataFrame = {
      data.filter(col("eventID") === "bladderCancer")
        .filter((col("nextDelta") < 3 and col("nextType") === "radiotherapy") or
        (col("previousDelta") < 3 and col("previousType") === "radiotherapy"))
    }
  }
}

object TargetDiseaseTransformer extends TargetDiseaseTransformer {
  override def transform(sources: Sources): Dataset[Event] = {

    val df = BladderCancerTransformer.transform(sources)
      .union(DcirActTransformer.transform(sources))
      .toDF

    import df.sqlContext.implicits._

    df.withDelta
      .withNextType
      .filterBladderCancer
      .select(outputColumns:_*)
      .as[Event]
      .union(McoActTransformer.transform(sources))
      .distinct
  }
}
