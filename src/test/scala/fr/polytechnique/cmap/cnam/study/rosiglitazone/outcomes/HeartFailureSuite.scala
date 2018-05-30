package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HeartFailureSuite extends SharedContext{

  "transform" should "return only found I50 found in main diagnosis" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientA", "I50", makeTS(2010, 1, 1)),
      MainDiagnosis("PatientB", "I50", makeTS(2010, 3, 10)),
      LinkedDiagnosis("PatientB", "I50", makeTS(2010, 2, 1)),
      AssociatedDiagnosis("PatientC", "I50", makeTS(2010, 3, 1)),

      MainDiagnosis("PatientD", "ABC", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "P50", makeTS(2010, 2, 1))
    ).toDS

    val expected = Seq(
      Outcome("PatientA", HeartFailure.outcomeName, makeTS(2010, 1, 1)),
      Outcome("PatientB", HeartFailure.outcomeName, makeTS(2010, 3, 10))
    ).toDS

    // When
    val result = HeartFailure.transform(input)

    // Then
    assertDSs(result, expected)
  }

  "transform" should "return only heart complication as main diagnosis with I50 as DR or DAS" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val input = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientA", "I110", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "I50", makeTS(2010, 1, 1)),

      MainDiagnosis("PatientB", "J81", makeTS(2010, 2, 1)),
      AssociatedDiagnosis("PatientB", "I50", makeTS(2010, 2, 1)),

      MainDiagnosis("PatientD", "ABC", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "ZZZ", makeTS(2010, 2, 1))
    ).toDS

    val expected = Seq(
      Outcome("PatientA", HeartFailure.outcomeName, makeTS(2010, 1, 1)),
      Outcome("PatientB", HeartFailure.outcomeName, makeTS(2010, 2, 1))
    ).toDS

    // When
    val result = HeartFailure.transform(input)

    // Then
    assertDSs(result, expected)
  }

  "checkHeartFailure" should "return only true when heart failures are found with respect to rules" in {

    //Given
    val input1 = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientA", "I110", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "I50", makeTS(2010, 1, 1))
    )
    val input2 = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientB", "J81", makeTS(2010, 2, 1)),
      AssociatedDiagnosis("PatientB", "I50", makeTS(2010, 2, 1))
    )
    val input3 =  Seq[Event[Diagnosis]](
      MainDiagnosis("PatientD", "ABC", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "ZZZ", makeTS(2010, 2, 1))
    )
    val input4 = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientE", "I50", makeTS(2010, 5, 1))
    )
    val input = List(input1, input2, input3, input4)

    val expected = List(true, true, false, true)

    //When
    val result = input.map(HeartFailure.checkHeartFailure)

    //Then
    assert(result == expected)
  }

  "findOutcomePerHospitalization" should "return Outcomes when the patient had heart failure with respect to the rules" in {

    //Given
    val input = Seq[Event[Diagnosis]](
      MainDiagnosis("PatientA", "hosp1", "I110", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "hosp1", "I50", makeTS(2010, 1, 1)),

      MainDiagnosis("PatientB", "hosp2", "J81", makeTS(2010, 2, 1)),
      AssociatedDiagnosis("PatientB", "hosp2", "I50", makeTS(2010, 2, 1)),

      MainDiagnosis("PatientD", "hosp3", "ABC", makeTS(2010, 1, 1)),
      LinkedDiagnosis("PatientA", "hosp3", "ZZZ", makeTS(2010, 2, 1)),

      MainDiagnosis("PatientE", "hosp4", "I50", makeTS(2010, 5, 1))
    )

    val expected = Stream(
      Outcome("PatientB", HeartFailure.outcomeName, makeTS(2010, 2, 1)),
      Outcome("PatientA", HeartFailure.outcomeName, makeTS(2010, 1, 1)),
      Outcome("PatientE", HeartFailure.outcomeName, makeTS(2010, 5, 1))
    )

    //When
    val result = HeartFailure.findOutcomesPerHospitalization(input)

    //Then
    assert(result == expected)
  }
}
