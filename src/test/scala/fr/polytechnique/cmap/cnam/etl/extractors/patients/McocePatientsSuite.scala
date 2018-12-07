package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.patients.McocePatients.McocePatientsImplicit
import fr.polytechnique.cmap.cnam.etl.implicits
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.study.fall.config.FallConfig
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McocePatientsSuite extends SharedContext {

  "calculateBirthYear" should "return a DataFrame with the birth year for each patient" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input = Seq(
      ("200410", 1, 79, makeTS(2014, 4, 18)),
      ("2004100010", 1, 73, makeTS(2014, 1, 9)),
      ("2004100010", 1, 73, makeTS(2014, 2, 11)),
      ("2004100010", 1, 74, makeTS(2014, 7, 18)),
      ("2004100010", 1, 74, makeTS(2014, 12, 12)),
      ("2004100010", 1, 74, makeTS(2014, 4, 15)),
      ("2004100010", 1, 74, makeTS(2014, 10, 27)),
      ("2004100010", 1, 74, makeTS(2014, 4, 4)),
      ("2004100010", 1, 74, makeTS(2014, 11, 6)),
      ("2004100010", 1, 74, makeTS(2014, 5, 2)),
      ("2004100010", 1, 74, makeTS(2014, 9, 26))
    ).toDF("patientID", "sex", "age", "event_date")

    val expected = Seq(
      ("200410", 1935),
      ("2004100010", 1940)
    ).toDF("patientID", "birth_year")

    //When
    val result = input.calculateBirthYear

    //Then
    assertDFs(result, expected)
  }

  "groupByIdAndAge" should "return a DataFrame with data aggregated by patient ID and age" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input = Seq(
      ("200410", 1, 79, makeTS(2014, 4, 18)),
      ("2004100010", 1, 73, makeTS(2014, 1, 9)),
      ("2004100010", 1, 73, makeTS(2014, 2, 11)),
      ("2004100010", 1, 74, makeTS(2014, 7, 18)),
      ("2004100010", 1, 74, makeTS(2014, 12, 12)),
      ("2004100010", 1, 74, makeTS(2014, 4, 15)),
      ("2004100010", 1, 74, makeTS(2014, 10, 27)),
      ("2004100010", 1, 74, makeTS(2014, 4, 4)),
      ("2004100010", 1, 74, makeTS(2014, 11, 6)),
      ("2004100010", 1, 74, makeTS(2014, 5, 2)),
      ("2004100010", 1, 74, makeTS(2014, 9, 26))
    ).toDF("patientID", "sex", "age", "event_date")

    val expected = Seq(
      ("200410", 79, 1.0, 1.0, makeTS(2014, 4, 18), makeTS(2014, 4, 18)),
      ("2004100010", 73, 2.0, 2.0, makeTS(2014, 1, 9), makeTS(2014, 2, 11)),
      ("2004100010", 74, 8.0, 8.0, makeTS(2014, 4, 4), makeTS(2014, 12, 12))
    ).toDF("patientID", "age", "sum_sex", "count_sex", "min_event_date", "max_event_date")

    //When
    val result = input.groupByIdAndAge

    //Then
    assertDFs(result, expected)

  }

  "calculateBirthDateAndGender" should "return a DataFrame with aggregated data" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val input = Seq(
      ("200410", 79, 1.0, 1.0, makeTS(2014, 4, 18), makeTS(2014, 4, 18), 1935),
      ("2004100010", 73, 2.0, 2.0, makeTS(2014, 1, 9), makeTS(2014, 2, 11), 1940),
      ("2004100010", 74, 8.0, 8.0, makeTS(2014, 4, 4), makeTS(2014, 12, 12), 1940)
    ).toDF("patientID", "age", "sum_sex", "count_sex", "min_event_date", "max_event_date", "birth_year")
    val expected = Seq(
      ("200410", 1, makeTS(1935, 4, 1)),
      ("2004100010", 1, makeTS(1940, 3, 1))
    ).toDF("patientID", "gender", "birthDate")

    //When
    val result = input.calculateBirthDateAndGender

    //Then
    assertDFs(result, expected)

  }

  "extract" should "return the correct data in a Dataset[Patient] for a known input" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val fallConfig = FallConfig.load("", "test")
    val patientsConfig = PatientsConfig(fallConfig.base.studyStart)
    import implicits.SourceReader
    val sources = Sources.sanitize(sqlContext.readSources(fallConfig.input))
    val expected = Seq(
      ("200410", 1, makeTS(1935, 4, 1)),
      ("2004100010", 1, makeTS(1940, 3, 1))
    ).toDF("patientID", "gender", "birthDate")
      .withColumn("deathDate", lit(null).cast(TimestampType))


    //When
    val result = McocePatients.extract(sources.mcoCe.get, patientsConfig.minGender,
      patientsConfig.maxGender, patientsConfig.minYear, patientsConfig.maxYear).toDF()

    //Then
    assertDFs(result, expected)

  }


}
