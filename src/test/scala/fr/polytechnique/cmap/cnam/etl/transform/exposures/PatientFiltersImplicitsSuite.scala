package fr.polytechnique.cmap.cnam.etl.transform.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.transform.exposures.Columns._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class PatientFiltersImplicitsSuite extends SharedContext {

  "filterEarlyDiagnosedPatients" should "drop patients who had a diagnostic within 6 months after follow-up start" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 10), makeTS(2006, 6, 1)),
      ("Patient_A", "disease", "C67", makeTS(2006, 5, 30), makeTS(2006, 6, 1)),
      ("Patient_B", "molecule", "", makeTS(2006, 1, 1), makeTS(2006, 6, 1)),
      ("Patient_B", "disease", "C67", makeTS(2006, 8, 1), makeTS(2006, 6, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1), makeTS(2006, 6, 1))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart)

    val expected = Seq(
      ("Patient_B", "molecule"),
      ("Patient_B", "disease"),
      ("Patient_C", "molecule")
    ).toDF(PatientID, Category)

    // When
    val instance = new PatientFiltersImplicits(input)
    val result = instance.filterEarlyDiagnosedPatients(doFilter = true, diseaseCode = "C67")
      .select(PatientID, Category)

    // Then
    assertDFs(result, expected)
  }

  it should "return the same data if we pass false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 10)),
      ("Patient_A", "disease", "C67", makeTS(2007, 1, 1))
    ).toDF(PatientID, Category, Value, Start)

    val expected = input

    // When
    val instance = new PatientFiltersImplicits(input)
    val result = instance.filterEarlyDiagnosedPatients(doFilter = false, diseaseCode = "C67")

    // Then
    assertDFs(result, expected)
  }

  "filterDelayedEntries" should "drop patients not exposed during the first N months of study" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val studyStart = makeTS(2006, 1, 1)
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 1)),
      ("Patient_A", "molecule", "", makeTS(2008, 2, 1)),
      ("Patient_B", "molecule", "", makeTS(2009, 1, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 2, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1))
    ).toDF(PatientID, Category, Value, Start)

    val expected = Seq(
      ("Patient_C", "molecule"),
      ("Patient_C", "molecule")
    ).toDF(PatientID, Category)

    // When
    val instance = new PatientFiltersImplicits(input)
    val result = instance.filterDelayedEntries(doFilter = true, studyStart = studyStart)
      .select(PatientID, Category)

    // Then
    assertDFs(result, expected)
  }

  it should "return the same data if we pass false" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val studyStart = makeTS(2006, 1, 1)
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 1)),
      ("Patient_B", "molecule", "", makeTS(2009, 1, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1))
    ).toDF(PatientID, Category, Value, Start)

    val expected = input

    // When
    val instance = new PatientFiltersImplicits(input)
    val result = instance.filterDelayedEntries(doFilter = false, studyStart = studyStart)

    // Then
    assertDFs(result, expected)
  }

  "filterPatients" should "filter correctly based on the arguments" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val studyStart = makeTS(2006, 1, 1)
    val input = Seq(
      ("Patient_A", "molecule", "", makeTS(2008, 1, 10), makeTS(2006, 6, 1)),
      ("Patient_A", "disease", "C67", makeTS(2006, 5, 30), makeTS(2006, 6, 1)),
      ("Patient_B", "molecule", "", makeTS(2006, 1, 1), makeTS(2006, 6, 1)),
      ("Patient_B", "molecule", "", makeTS(2006, 2, 1), makeTS(2006, 6, 1)),
      ("Patient_B", "disease", "C67", makeTS(2006, 8, 1), makeTS(2006, 6, 1)),
      ("Patient_C", "molecule", "", makeTS(2006, 1, 1), makeTS(2006, 6, 1)),
      ("Patient_D", "molecule", "", makeTS(2008, 1, 1), makeTS(2008,1,1)),
      ("Patient_D", "molecule", "", makeTS(2008, 2, 1), makeTS(2008,1,1)),
      ("Patient_E", "molecule", "", makeTS(2009, 1, 1), makeTS(2008,1,1)),
      ("Patient_F", "molecule", "", makeTS(2006, 2, 1), makeTS(2008,1,1)),
      ("Patient_F", "molecule", "", makeTS(2006, 1, 1), makeTS(2008,1,1))
    ).toDF(PatientID, Category, Value, Start, FollowUpStart)

    val expected = Seq(
      ("Patient_B", "molecule"),
      ("Patient_B", "molecule"),
      ("Patient_B", "disease"),
      ("Patient_C", "molecule"),
      ("Patient_F", "molecule"),
      ("Patient_F", "molecule")
    ).toDF(PatientID, Category)

    // When
    val instance = new PatientFiltersImplicits(input)
    val result = instance.filterPatients(
      studyStart = studyStart,
      diseaseCode = "C67",
      delayedEntries = true,
      earlyDiagnosed = true
    ).select(PatientID, Category)

    // Then
    assertDFs(result, expected)
  }
}
