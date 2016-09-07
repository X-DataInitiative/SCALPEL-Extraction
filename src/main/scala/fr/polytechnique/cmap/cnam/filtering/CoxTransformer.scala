package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

// Start and End are expressed in month from the patient startObs
case class CoxFeature(
  PatientID: String,
  Gender: Int,
  AgeGroup: String,
  Start: Int,
  End: Int,
  Insuline: Double = 0,
  Sulfonylurea: Double = 0,
  Metformine: Double = 0,
  Pioglitazone: Double = 0,
  Rosiglitazone: Double = 0,
  Other: Double = 0,
  hasCancer: Double = 0)

//object CoxTransformer extends DatasetTransformer[FlatEvent, CoxFeature] {
//
//  val exposuresCols = Seq(
//    col("patientID"),
//    col("gender"),
//    col("birthDate"),
//    col("deathDate"),
//    col("eventId").as("moleculeName"),
//    col("start"),
//    col("end")
//  )
//
//  implicit class FlatEventDataFrame(data: DataFrame) {
//
//    def stackDates = {
//      data
//        .select(exposuresCols: _*).withColumn("stackedDate", col("start"))
//        .unionAll(data.select(exposuresCols: _*).withColumn("start", col("end")))
//    }
//
//    def normalizeDates
//  }
//
//  def transform(input: Dataset[FlatEvent]): Dataset[CoxFeature] = {
//    val partitionedInput = input.toDF.repartition(col("category"))
//
//    val rawExposures = partitionedInput
//      .where(col("category") === "exposure")
//      .repartition(col("patientID"))
//      .persist
//
//    // Resulting schema: (patientID, date)
//    val patientsWithDiseaseDate = partitionedInput
//      .where(col("category") === "disease")
//      .repartition(col("patientID"))
//      .groupBy("patientID").agg(min("start").as("diseaseDate"))
//      .persist
//
//    val exposures = rawExposures.alias("l").join(
//      patientsWithDiseaseDate.alias("r"),
//      col("l.patientID") === col("r.patientID") && (col("l.end") === col("r.diseaseDate")),
//      "left_outer"
//    ).select(exposuresCols :+ col("diseaseDate") :_*)
//
//    val result = exposures
//      .stackDates
//      .normalizeDates
//      //.addLeadDates
//      .pivotMolecules
//  }
//}
