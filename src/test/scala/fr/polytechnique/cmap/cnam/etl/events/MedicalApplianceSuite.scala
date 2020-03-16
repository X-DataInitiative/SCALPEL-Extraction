// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec

class MedicalApplianceSuite extends AnyFlatSpec {

  val patientID: String = "patientID"
  val group: String = "mockGroup"
  val start: Timestamp = mock(classOf[Timestamp])
  val end: Timestamp = mock(classOf[Timestamp])

  "apply" should "allow creation of a Dcir Medical Appliance event" in {
    // Given
    val expected = Event[DcirMedicalAppliance.type](patientID, DcirMedicalAppliance.category, group, "30125", 1.0, start, Some(end))
    // When
    val result = DcirMedicalAppliance(patientID, group, "30125", 1.0, start, end)
    // Then
    assert(result == expected)
  }
}
