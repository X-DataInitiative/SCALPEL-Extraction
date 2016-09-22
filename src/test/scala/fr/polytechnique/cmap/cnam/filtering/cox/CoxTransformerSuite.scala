package fr.polytechnique.cmap.cnam.filtering.cox

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.filtering.FlatEvent
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions._

class CoxTransformerSuite extends SharedContext {

  "withAge" should "add a column with the age of the patient" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", makeTS(1950, 1, 1), makeTS(2006, 7, 15)),
      ("Patient_A", makeTS(1950, 1, 1), makeTS(2006, 7, 15)),
      ("Patient_B", makeTS(1960, 7, 15), makeTS(2006, 4, 21)),
      ("Patient_B", makeTS(1960, 7, 15), makeTS(2006, 4, 21)),
      ("Patient_C", makeTS(1970, 12, 31), makeTS(2006, 1, 1)),
      ("Patient_C", makeTS(1970, 12, 31), makeTS(2006, 1, 1)),
      ("Patient_D", makeTS(1970, 12, 31), makeTS(2006, 12, 31)),
      ("Patient_D", makeTS(1970, 12, 31), makeTS(2006, 12, 31))
    ).toDF("patientID", "birthDate", "followUpStart")

    val expected = Seq(
      ("Patient_A", 683),
      ("Patient_A", 683),
      ("Patient_B", 557),
      ("Patient_B", 557),
      ("Patient_C", 432),
      ("Patient_C", 432),
      ("Patient_D", 432),
      ("Patient_D", 432)
    ).toDF("patientID", "age")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.withAge.select("patientID", "age")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }


  "withAgeGroups" should "add a column for each age group" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 1, 678),
      ("Patient_A", 1, 678),
      ("Patient_A", 1, 678),
      ("Patient_A", 1, 678),
      ("Patient_B", 1, 554),
      ("Patient_B", 1, 554)
    ).toDF("patientID", "gender", "age")

    val expected = Seq(
      ("Patient_A", 1, 678, "55-59"),
      ("Patient_A", 1, 678, "55-59"),
      ("Patient_A", 1, 678, "55-59"),
      ("Patient_A", 1, 678, "55-59"),
      ("Patient_B", 1, 554, "45-49"),
      ("Patient_B", 1, 554, "45-49")
    ).toDF("patientID", "gender", "age", "ageGroup")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.withAgeGroup

    // Then
    import RichDataFrames._
    result.printSchema
    expected.printSchema
    result.orderBy("patientID").show
    expected.orderBy("patientID").show
    assert(result === expected)
  }

  "withHasCancer" should "add a column with 1 for the values who had a disease as reason for follow-up end" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "disease"),
      ("Patient_A", "disease"),
      ("Patient_B", "death"),
      ("Patient_B", "death"),
      ("Patient_C", "trackloss"),
      ("Patient_C", "trackloss"),
      ("Patient_D", "observationEnd"),
      ("Patient_D", "observationEnd")
    ).toDF("patientID", "endReason")

    val expected = Seq(
      ("Patient_A", 1),
      ("Patient_A", 1),
      ("Patient_B", 0),
      ("Patient_B", 0),
      ("Patient_C", 0),
      ("Patient_C", 0),
      ("Patient_D", 0),
      ("Patient_D", 0)
    ).toDF("patientID", "hasCancer")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.withHasCancer.select("patientID", "hasCancer")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "normalizeDates" should "normalize start and end of exposure based on the follow-up start" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", makeTS(2007, 1, 1), makeTS(2007, 1, 1), makeTS(2007, 10, 2)),
      ("Patient_A", makeTS(2007, 1, 1), makeTS(2007, 4, 1), makeTS(2007, 6, 2)),
      ("Patient_A", makeTS(2007, 1, 1), makeTS(2007, 9, 1), makeTS(2007, 10, 2)),
      ("Patient_B", makeTS(2007, 6, 1), makeTS(2007, 10, 1), makeTS(2007, 12, 2)),
      ("Patient_B", makeTS(2007, 6, 1), makeTS(2007, 11, 1), makeTS(2007, 12, 2)),
      ("Patient_B", makeTS(2007, 6, 1), makeTS(2007, 12, 1), makeTS(2007, 12, 2))
    ).toDF("patientID", "followUpStart", "start", "end")

    val expected = Seq(
      ("Patient_A", 0, 9),
      ("Patient_A", 3, 5),
      ("Patient_A", 8, 9),
      ("Patient_B", 4, 6),
      ("Patient_B", 5, 6),
      ("Patient_B", 6, 6)
    ).toDF("patientID", "start", "end")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.normalizeDates.select("patientID", "start", "end")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "stackDates" should "union the dataframe with itself, creating a new column with start stacked with end" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 0, 9),
      ("Patient_A", 3, 5),
      ("Patient_A", 8, 9),
      ("Patient_B", 4, 6),
      ("Patient_B", 5, 6),
      ("Patient_B", 6, 6)
    ).toDF("patientID", "start", "end")

    val expected = Seq(
      ("Patient_A", 0),
      ("Patient_A", 9),
      ("Patient_A", 3),
      ("Patient_A", 5),
      ("Patient_A", 8),
      ("Patient_A", 9),
      ("Patient_B", 4),
      ("Patient_B", 6),
      ("Patient_B", 5),
      ("Patient_B", 6),
      ("Patient_B", 6),
      ("Patient_B", 6)
    ).toDF("patientID", "coxStart")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.stackDates.select("patientID", "coxStart")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "withCoxEnd" should "add a column with the normalized exposure end calculated from the normalized start" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 0),
      ("Patient_A", 9),
      ("Patient_A", 3),
      ("Patient_A", 5),
      ("Patient_A", 8),
      ("Patient_A", 9),
      ("Patient_B", 4),
      ("Patient_B", 6),
      ("Patient_B", 5),
      ("Patient_B", 6),
      ("Patient_B", 6),
      ("Patient_B", 6)
    ).toDF("patientID", "coxStart")

    val expected = Seq(
      ("Patient_A", 0, Some(3)),
      ("Patient_A", 3, Some(5)),
      ("Patient_A", 5, Some(8)),
      ("Patient_A", 8, Some(9)),
      ("Patient_A", 9, None),
      ("Patient_A", 9, None),
      ("Patient_B", 4, Some(5)),
      ("Patient_B", 5, Some(6)),
      ("Patient_B", 6, None),
      ("Patient_B", 6, None),
      ("Patient_B", 6, None),
      ("Patient_B", 6, None)
    ).toDF("patientID", "coxStart", "coxEnd")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.withCoxEnd.select("patientID", "coxStart", "coxEnd")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "prepareToPivot" should "insert a line for each molecule that was exposed between normalizedStart and normalizedEnd" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 1, 0, 3),
      ("Patient_A", 1, 3, 5),
      ("Patient_A", 1, 5, 8),
      ("Patient_A", 1, 8, 9),
      ("Patient_B", 1, 4, 5),
      ("Patient_B", 1, 5, 6)
    ).toDF("patientID", "gender", "coxStart", "coxEnd")

    val exposures = Seq(
      ("Patient_A", "METFORMINE", 0, 9),
      ("Patient_A", "INSULINE", 3, 5),
      ("Patient_A", "PIOGLITAZONE", 8, 9),
      ("Patient_B", "INSULINE", 4, 6),
      ("Patient_B", "OTHER", 5, 6),
      ("Patient_B", "PIOGLITAZONE", 6, 6)
    ).toDF("patientID", "moleculeName", "start", "end")

    val expected = Seq(
      ("Patient_A", 1, "METFORMINE", 0, 3),
      ("Patient_A", 1, "INSULINE", 3, 5),
      ("Patient_A", 1, "METFORMINE", 3, 5),
      ("Patient_A", 1, "METFORMINE", 5, 8),
      ("Patient_A", 1, "PIOGLITAZONE", 8, 9),
      ("Patient_A", 1, "METFORMINE", 8, 9),
      ("Patient_B", 1, "INSULINE", 4, 5),
      ("Patient_B", 1, "OTHER", 5, 6),
      ("Patient_B", 1, "INSULINE", 5, 6)
    ).toDF("patientID", "gender", "moleculeName", "coxStart", "coxEnd")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.prepareToPivot(exposures)
      .select("patientID", "gender", "moleculeName", "coxStart", "coxEnd")

    // Then
    import RichDataFrames._
    result.show
    expected.show
    assert(result === expected)
  }

  "pivotMolecules" should "pivot the values in the molecule column into six different columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 1, 678, "55-59", "METFORMINE", 0, 3, 1),
      ("Patient_A", 1, 678, "55-59", "INSULINE", 3, 5, 1),
      ("Patient_A", 1, 678, "55-59", "METFORMINE", 3, 5, 1),
      ("Patient_A", 1, 678, "55-59", "METFORMINE", 5, 8, 1),
      ("Patient_A", 1, 678, "55-59", "PIOGLITAZONE", 8, 9, 1),
      ("Patient_A", 1, 678, "55-59", "METFORMINE", 8, 9, 1),
      ("Patient_B", 1, 554, "45-49", "INSULINE", 4, 5, 0),
      ("Patient_B", 1, 554, "45-49", "OTHER", 5, 6, 0),
      ("Patient_B", 1, 554, "45-49", "INSULINE", 5, 6, 0)
    ).toDF("patientID", "gender", "age", "ageGroup", "moleculeName", "coxStart", "coxEnd", "hasCancer")

    val expected = Seq(
      ("Patient_A", 1, 678, "55-59", 0, 3, 1, 0, 0, 1, 0, 0, 0),
      ("Patient_A", 1, 678, "55-59", 3, 5, 1, 1, 0, 1, 0, 0, 0),
      ("Patient_A", 1, 678, "55-59", 5, 8, 1, 0, 0, 1, 0, 0, 0),
      ("Patient_A", 1, 678, "55-59", 8, 9, 1, 0, 0, 1, 1, 0, 0),
      ("Patient_B", 1, 554, "45-49", 4, 5, 0, 1, 0, 0, 0, 0, 0),
      ("Patient_B", 1, 554, "45-49", 5, 6, 0, 1, 0, 0, 0, 0, 1)
    ).toDF("patientID", "gender", "age", "ageGroup", "start", "end", "hasCancer", "insuline",
      "sulfonylurea", "metformine", "pioglitazone", "rosiglitazone", "other")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.pivotMolecules

    // Then
    import RichDataFrames._
    result.printSchema
    expected.printSchema
    result.orderBy("patientID", "start", "end").show
    expected.orderBy("patientID", "start", "end").show
    assert(result === expected)
  }

  "fixCancerValues" should "leave only the last feature of each patient with 1 for the hasCancer value" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", 1, 0, 3, 1),
      ("Patient_A", 1, 3, 5, 1),
      ("Patient_A", 1, 5, 8, 1),
      ("Patient_A", 1, 8, 9, 1),
      ("Patient_B", 1, 4, 5, 0),
      ("Patient_B", 1, 5, 6, 0)
    ).toDF("patientID", "gender", "start", "end", "hasCancer")

    val expected = Seq(
      ("Patient_A", 1, 0, 3, 0),
      ("Patient_A", 1, 3, 5, 0),
      ("Patient_A", 1, 5, 8, 0),
      ("Patient_A", 1, 8, 9, 1),
      ("Patient_B", 1, 4, 5, 0),
      ("Patient_B", 1, 5, 6, 0)
    ).toDF("patientID", "gender", "start", "end", "hasCancer")

    // When
    import fr.polytechnique.cmap.cnam.filtering.cox.CoxTransformer.CoxDataFrame
    val result = input.adjustCancerValues

    // Then
    import RichDataFrames._
    result.printSchema
    expected.printSchema
    result.orderBy("patientID", "start", "end").show
    expected.orderBy("patientID", "start", "end").show
    assert(result === expected)
  }

  // TODO: Improve this test case ( not enough time right now =/ )
  "transform" should "do everything right" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "followUpPeriod",
        "disease", 900.0, makeTS(2007, 1, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "followUpPeriod",
        "trackloss", 900.0, makeTS(2006, 7, 1), Some(makeTS(2008, 9, 1))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2007, 5, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_A", 1, makeTS(1950, 1, 1), Some(makeTS(2009, 7, 11)), "exposure",
        "SULFONYLUREA", 1.0, makeTS(2008, 8, 1), Some(makeTS(2009, 7, 11))),
      FlatEvent("Patient_B", 1, makeTS(1940, 1, 1), Some(makeTS(2008, 9, 1)), "exposure",
        "PIOGLITAZONE", 1.0, makeTS(2006, 8, 1), Some(makeTS(2008, 9, 1)))
    ).toDS

    val expected = Seq(
      CoxFeature("Patient_A", 1, 683, "55-59", 19, 30, 1, 0, 1, 0, 1, 0),
      CoxFeature("Patient_A", 1, 683, "55-59", 4, 19, 0, 0, 0, 0, 1, 0),
      CoxFeature("Patient_B", 1, 803, "65-69", 1, 26, 0, 0, 0, 0, 1, 0)
    ).toDF

    // When
    val result = CoxTransformer.transform(input).toDF

    // Then
    import RichDataFrames._
    result.printSchema
    expected.printSchema
    result.orderBy("patientID", "start", "end").show
    expected.orderBy("patientID", "start", "end").show
    assert(result === expected)
  }
}
