package fr.polytechnique.cmap.cnam.etl.transformer.exposure

import org.apache.spark.sql.DataFrame
import org.mockito.Mockito.mock
import org.scalatest.FlatSpec

class PatientFiltersSuite extends FlatSpec {

  "addFiltersImplicits" should "return the correct implementation filtering strategy" in {
    val instance = new PatientFilters{}.addFiltersImplicits(mock(classOf[DataFrame]))
    assert(instance.isInstanceOf[PatientFiltersImplicits])
  }
}