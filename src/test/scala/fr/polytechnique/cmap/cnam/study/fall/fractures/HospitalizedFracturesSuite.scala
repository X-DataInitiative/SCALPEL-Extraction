package fr.polytechnique.cmap.cnam.study.fall.fractures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions._


class HospitalizedFracturesSuite extends SharedContext {

  "isInCodeList" should "return yes if there is a code with the right start" in {
    // Given
    val event = MainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18))
    val codes = Set("jam", "bon", "de", "bayonne")

    // When
    val result = HospitalizedFractures.isInCodeList(event, codes)

    // Then
    assert(result)
  }

  it should "return yes if there is an exact same code" in {
    // Given
    val event = MainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18))
    val codes = Set("jambe cassée", "bon", "de", "bayonne")

    // When
    val result = HospitalizedFractures.isInCodeList(event, codes)

    // Then
    assert(result)
  }

  it should "return no if there is no correct code" in {
    // Given
    val event = MainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18))
    val codes = Set("avada kedavra", "bon", "de", "bayonne")

    // When
    val result = HospitalizedFractures.isInCodeList(event, codes)

    // Then
    assert(!result)
  }

  "isFractureDiagnosis" should "return yes for correct CIM10 code" in {
    // Given
    val event = MainDiagnosis("Pierre", "3", "S02.35", makeTS(2017, 7, 18))

    // When
    val result = HospitalizedFractures.isFractureDiagnosis(event, AllSites.codesCIM10)

    // Then
    assert(result)
  }

  "isMainDiagnosis" should "return yes for correct DP code" in {
    // Given
    val event = MainDiagnosis("Pierre", "3", "whatever", makeTS(2017, 7, 18))

    // When
    val result = HospitalizedFractures.isMainOrDASDiagnosis(event)

    // Then
    assert(result)
  }

  it should "return no for other code" in {
    // Given
    val event = LinkedDiagnosis("Pierre", "3", "whatever", makeTS(2017, 7, 18))

    // When
    val result = HospitalizedFractures.isMainOrDASDiagnosis(event)

    // Then
    assert(!result)
  }

  "isBadGHM" should "return yes for correct GHM code" in {
    // Given
    val event = McoCCAMAct("Pierre", "3", "LJGA001", makeTS(2017, 7, 18))

    // When
    val result = HospitalizedFractures.isBadGHM(event)

    // Then
    assert(result)
  }

  "filterHospitalStay" should "return correct dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = List(
      MainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20)),
      MainDiagnosis("Pierre", "3", "jambe cassée", makeTS(2017, 7, 18))
    ).toDS

    val badStays = Seq(
      HospitalStayID("Pierre", "3")
    ).toDS

    val expected = List(
      MainDiagnosis("Paul", "1", "hemorroides", makeTS(2017, 7, 20))
    ).toDS

    // When
    val result = HospitalizedFractures.filterHospitalStay(input, badStays)

    // Then
    assertDSs(result, expected)
  }

  "transform" should "return correct Outcome dataset" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val diagnoses = Seq(
      MainDiagnosis("Pierre", "3", "S02.42", makeTS(2017, 7, 18)),
      MainDiagnosis("Paul", "1", "S42.54678", makeTS(2017, 7, 20)),
      MainDiagnosis("Paul", "7", "hemorroides", makeTS(2017, 1, 2)),
      AssociatedDiagnosis("Jacques", "8", "qu'est-ce-que tu fais là?", makeTS(2017, 7, 18))
    ).toDS

    val medicalActs = Seq(
      McoCCAMAct("Paul", "1", "LJGA001", makeTS(2017, 7, 20))
    ).toDS

    val expected = Seq(
      Outcome("Pierre", "AllSites", "hospitalized_fall", makeTS(2017, 7, 18))
    ).toDS

    // When
    val result = HospitalizedFractures.transform(diagnoses, medicalActs, List(AllSites))
    // Then
    assertDSs(result, expected)
  }

}
