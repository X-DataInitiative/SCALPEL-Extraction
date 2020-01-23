// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HospitalStaySuite extends SharedContext {

  "apply" should "allow creation of hospital stay event" in {

    //given
    val expected = Event(
      "patientID", HospitalStay.category, "hospitalID", "hospitalID", 0D,
      makeTS(2018, 1, 1), Some(makeTS(2018, 3, 1))
    )
    //when
    val result = HospitalStay(
      "patientID", "hospitalID",
      makeTS(2018, 1, 1), makeTS(2018, 3, 1)
    )
    //then
    assert(expected == result)

  }
}
