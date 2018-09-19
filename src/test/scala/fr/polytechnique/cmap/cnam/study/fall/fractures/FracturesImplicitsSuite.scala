package fr.polytechnique.cmap.cnam.study.fall.fractures

import me.danielpes.spark.datetime.implicits._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, Outcome}
import fr.polytechnique.cmap.cnam.study.fall.FracturesTransformerImplicits._
import fr.polytechnique.cmap.cnam.study.fall.PrivateAmbulatoryFractures
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class FracturesImplicitsSuite extends SharedContext{
  "groupConsecutiveFractures" should "group fractures when the start date is in the same frame" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val data = Seq(
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2008, 2, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 5, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 3, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientB", "Doigt", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 3, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 5, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 7, 1))
    ).toDF.as[Event[Outcome]]

    val expected = Seq(
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2008, 2, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientB", "Doigt", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 3, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 5, 1))
    ).toDF.as[Event[Outcome]]

    //When
    val result = data.groupConsecutiveFractures(3.months)

    //Then
    assertDSs(result, expected)
  }

  it should " not group fractures when the fall frame is 0" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val data = Seq(
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2008, 2, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 5, 1)),
      Outcome("patientA", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 3, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 1, 1)),
      Outcome("patientB", "Doigt", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 3, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 5, 1)),
      Outcome("patientB", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, makeTS(2007, 7, 1))
    ).toDF.as[Event[Outcome]]
    val expected = data
    //When
    val result = data.groupConsecutiveFractures(0.months)
    //Then
    assertDSs(result, expected)
  }
}
