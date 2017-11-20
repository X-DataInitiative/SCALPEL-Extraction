package fr.polytechnique.cmap.cnam.etl.filters

import fr.polytechnique.cmap.cnam.etl.patients.Patient
import org.apache.spark.sql.Dataset
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class PatientFiltersSuite extends FlatSpec {

      "addFiltersImplicits" should "return the correct implementation filtering strategy" in {
        val instance = PatientFilters.addImplicits(mock(classOf[Dataset[Patient]]))
        assert(instance.isInstanceOf[PatientFiltersImplicits])
      }
  }
