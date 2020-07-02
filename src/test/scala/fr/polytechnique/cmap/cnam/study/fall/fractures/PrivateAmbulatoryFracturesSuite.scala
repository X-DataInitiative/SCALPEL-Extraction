// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.fractures

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{DcirAct, Event, Outcome}
import fr.polytechnique.cmap.cnam.util.functions._

class PrivateAmbulatoryFracturesSuite extends SharedContext {

  "containsNonHospitalizedCcam" should "return true if the event value is in the list of codes" in {
    // Given
    val eventCode = "NBEP002"
    val event = DcirAct("george", "stupidcode", eventCode, makeTS(2007, 1, 1))

    // When
    val result = PrivateAmbulatoryFractures.containsNonHospitalizedCcam(event)

    // Then
    assert(result)
  }

  it should "return false otherwise" in {
    // Given
    val eventCode = "Weird Code"
    val event = DcirAct("george", "stupidcode", eventCode, makeTS(2007, 1, 1))

    // When
    val result = PrivateAmbulatoryFractures.containsNonHospitalizedCcam(event)

    // Then
    assert(!result)
  }

  "isPrivateAmbulatory" should "return true for correct even" in {
    // Given
    val event = DcirAct("george", DcirAct.groupID.PrivateAmbulatory, "stupidcode", makeTS(2007, 1, 1))

    // When
    val result = PrivateAmbulatoryFractures.isPrivateAmbulatory(event)

    // Then
    assert(result)
  }

  "transform" should "return correct outcomes" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val input = Seq(
      DcirAct("riri", DcirAct.groupID.PrivateAmbulatory, "NBEP002", makeTS(2007, 1, 1)),
      DcirAct("fifi", DcirAct.groupID.PrivateAmbulatory, "stupidcode", makeTS(2007, 1, 1)),
      DcirAct("loulou", DcirAct.groupID.PublicAmbulatory, "stupidcode", makeTS(2007, 1, 1))
    ).toDS

    val expected: Dataset[Event[Outcome]] = Seq[Event[Outcome]](
      Outcome("riri", "FemurExclusionCol", PrivateAmbulatoryFractures.outcomeName, 1D, makeTS(2007, 1, 1), None)
    ).toDS

    // When
    val result = PrivateAmbulatoryFractures.transform(input)

    // Then
    assertDSs(result, expected)

  }

}
