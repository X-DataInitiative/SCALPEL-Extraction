// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.datatypes._
import fr.polytechnique.cmap.cnam.util.functions

class ExposureDurationSuite extends SharedContext{
  "+" should "return a RemainingPeriod of type Period indicating the addition of Periods" in {
    val input = ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)
    var result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5)), 0L)
    var expected: RemainingPeriod[ExposureDuration] = RightRemainingPeriod(
      ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)
    )
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1)), 0L))
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L))
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 15)), 0L))
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 12, 1)), 0L))
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1)), 0L)
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1)), 0L)),
      RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L))
    )
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31)), 0L)
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31)), 0L)),
      RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L))
    )
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)), 0L)
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)),
      RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)), 0L))
    )
    assert(result == expected)

    result = input + ExposureDuration("a", "b", Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)), 0L)
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)),
      RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)), 0L))
    )
    assert(result == expected)

    result = input + ExposureDuration("d", "b", Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L))
    assert(result == expected)

    result = input + ExposureDuration("a", "f", Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)), 0L)
    expected = RightRemainingPeriod(ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L))
    assert(result == expected)

  }
}
