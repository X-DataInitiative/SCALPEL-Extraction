// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.rosiglitazone.outcomes

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Diagnosis, Event, MainDiagnosis, Outcome}
import fr.polytechnique.cmap.cnam.study.rosiglitazone.RosiglitazoneStudyCodes
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class RosiglitazoneOutcomeTransformerSuite extends SharedContext {

  "outcomeName" should "match the definition outcome name" in {
    // Given
    val infarctus = OutcomeDefinition.Infarctus
    val heartFailure = OutcomeDefinition.HeartFailure

    // When
    val infarctusTransformer = new RosiglitazoneOutcomeTransformer(infarctus)
    val heartFailureTransformer = new RosiglitazoneOutcomeTransformer(heartFailure)

    // Then
    assert(infarctusTransformer.outcomeName == infarctus.outcomeName)
    assert(heartFailureTransformer.outcomeName == heartFailure.outcomeName)
  }

  "transform" should "return the correct Infarctus outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    object RosiglitazoneCodes extends RosiglitazoneStudyCodes
    val infarctusCodes = RosiglitazoneCodes.infarctusDiagnosisCodes
    val input: Dataset[Event[Diagnosis]] = Seq(
      MainDiagnosis("A", infarctusCodes.head, makeTS(2010, 1, 1))
    ).toDS
    val expected: Dataset[Event[Outcome]] = Seq(
      Outcome("A", OutcomeDefinition.Infarctus.outcomeName, makeTS(2010, 1, 1))
    ).toDS

    // When
    val result = new RosiglitazoneOutcomeTransformer(OutcomeDefinition.Infarctus).transform(input)

    // Then
    assertDSs(result, expected)
  }
}
