// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.filters

import org.mockito.Mockito.mock
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.etl.patients.Patient

class PatientFiltersSuite extends AnyFlatSpec {

  "addFiltersImplicits" should "return the correct implementation filtering strategy" in {
    val instance = PatientFilters.addPatientsImplicits(mock(classOf[Dataset[Patient]]))
    assert(instance.isInstanceOf[PatientFiltersImplicits])
  }
}
