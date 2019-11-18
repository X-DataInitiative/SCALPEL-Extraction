// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.interaction

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Interaction
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class ExposureNSuite extends SharedContext {
  "-" should "return a RemainingPeriod of type ExposureN indicating the subtract of Periods" in {
    val input = ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))
    var result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5)))
    var expected: RemainingPeriod[ExposureN] = RightRemainingPeriod(
      ExposureN("A", Set("A", "B"), Period(
        makeTS(2019, 11, 5),
        makeTS(2019, 12, 1)
      ))
    )
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = NullRemainingPeriod
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 5), makeTS(2019, 11, 15)))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5)))),
      RightRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 15), makeTS(2019, 12, 1))))
    )
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 5), makeTS(2019, 12, 15)))
    expected = LeftRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5))))
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 10, 5), makeTS(2019, 11, 15)))
    expected = RightRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 15), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 10, 1), makeTS(2019, 11, 1)))
    expected = RightRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 10, 1), makeTS(2019, 10, 31)))
    expected = RightRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 12, 5), makeTS(2019, 12, 15)))
    expected = LeftRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B"), Period(makeTS(2019, 12, 1), makeTS(2019, 12, 15)))
    expected = LeftRemainingPeriod(ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input - ExposureN("B", Set("A", "B"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = NullRemainingPeriod
    assert(result == expected)

    result = input - ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = NullRemainingPeriod
    assert(result == expected)
  }

  "compare" should "compare two ExposureN based on the size of their values" in {
    val input = ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))
    var result = ExposureN.compare(input, ExposureN("A", Set("A", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5))))

    assert(result == 0)

    result = ExposureN.compare(input, ExposureN("A", Set("A"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5))))
    assert(result == 1)

    result = ExposureN.compare(input, ExposureN("A", Set("A", "C", "D"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5))))
    assert(result == -1)
  }

  "toLowerLevelInvolvedExposureN" should "give the Set of all possible ExposureN of level n-1 that are implicated in forming on the current ExposureN of level n" in {
    val input = ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))

    val expected = Set(
      ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))),
      ExposureN("A", Set("A", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))),
      ExposureN("A", Set("B", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))
    )

    val result = input.toLowerLevelInvolvedExposureN.toSet

    assert(expected == result)
  }

  "toInteraction" should "give the equivalent Interaction Event from the ExposureN" in {
    val input = Set(
      ExposureN("A", Set("A", "B", "D"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))),
      ExposureN("A", Set("A", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))),
      ExposureN("A", Set("B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))
    )

    val expected = Set(
      Interaction("A", "A_B_D", 3.0D, makeTS(2019, 11, 1), makeTS(2019, 12, 1)),
      Interaction("A", "A_C", 2.0D, makeTS(2019, 11, 1), makeTS(2019, 12, 1)),
      Interaction("A", "B", 1.0D, makeTS(2019, 11, 1), makeTS(2019, 12, 1))
    )

    val result = input.map(_.toInteraction)

    assert(expected == result)
  }

  "intersect" should "return an Option on ExposureN that represents elevating the current ExposureN by other ExposureN" in {
    val input = ExposureN("A", Set("A", "B"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1)))
    var result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5)))

    var expected: Option[ExposureN] = Some(ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 5))))
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = Some(ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 11, 5), makeTS(2019, 11, 15)))
    expected = Some(ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 5), makeTS(2019, 11, 15))))
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 11, 5), makeTS(2019, 12, 15)))
    expected = Some(ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 5), makeTS(2019, 12, 1))))
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 10, 5), makeTS(2019, 11, 15)))
    expected = Some(ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 11, 1), makeTS(2019, 11, 15))))
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 10, 1), makeTS(2019, 11, 1)))
    expected = None
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 10, 1), makeTS(2019, 10, 31)))
    expected = None
    assert(result == expected)

    result = input intersect ExposureN("A", Set("A", "B"), Period(makeTS(2019, 12, 5), makeTS(2019, 12, 15)))
    expected = None
    assert(result == expected)

    result = input intersect ExposureN("A", Set("C"), Period(makeTS(2019, 12, 1), makeTS(2019, 12, 15)))
    expected = None
    assert(result == expected)

    result = input intersect ExposureN("B", Set("A", "B"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = None
    assert(result == expected)

    result = input intersect  ExposureN("A", Set("A", "B", "C"), Period(makeTS(2019, 10, 1), makeTS(2019, 12, 1)))
    expected = None
    assert(result == expected)
  }
}
