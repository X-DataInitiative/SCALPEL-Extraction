// License: BSD 3 clause

// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.datatypes

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.functions


class PeriodSuite extends SharedContext {

  "&" should "return a boolean that indicates if two Periods intersect" in {
    val input1 = Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))
    var result = input1 & Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5))
    assert(result)
    result = input1 & Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1))
    assert(result)

    result = input1 & Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15))
    assert(result)
    result = input1 & Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15))
    assert(result)

    result = input1 & Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15))
    assert(result)

    result = !(input1 & Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1)))
    assert(result)
    result = !(input1 & Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31)))
    assert(result)
    result = !(input1 & Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)))
    assert(result)
    result = !(input1 & Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)))
    assert(result)

  }

  "+" should "return a RemainingPeriod of type Period indicating the addition of Periods" in {
    val input = Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))
    var result = input - Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5))
    var expected: RemainingPeriod[Period] = RightRemainingPeriod(
      Period(
        functions.makeTS(2019, 11, 5),
        functions.makeTS(2019, 12, 1)
      )
    )
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 15)))
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1))),
      RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    )
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31))),
      RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    )
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))),
      RightRemainingPeriod(Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)))
    )
    assert(result == expected)

    result = input + Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))),
      RightRemainingPeriod(Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)))
    )
    assert(result == expected)

  }

  "-" should "return a RemainingPeriod of type Period indicating the substract of Periods" in {
    val input = Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))
    var result = input - Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5))
    var expected: RemainingPeriod[Period] = RightRemainingPeriod(
      Period(
        functions.makeTS(2019, 11, 5),
        functions.makeTS(2019, 12, 1)
      )
    )
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1))
    expected = NullRemainingPeriod
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15))
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5))),
      RightRemainingPeriod(Period(functions.makeTS(2019, 11, 15), functions.makeTS(2019, 12, 1)))
    )
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15))
    expected = LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5)))
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 11, 15), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31))
    expected = RightRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15))
    expected = LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input - Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15))
    expected = LeftRemainingPeriod(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

  }

  "intersect" should "return a Period where both parent Periods are a sub period of the result" in {
    val input = Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1))
    var result = input intersect Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5))
    var expected: Option[Period] = Some(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5)))
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1))
    expected = Some(input)
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15))
    expected = Some(Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15)))
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15))
    expected = Some(Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 1)))
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15))
    expected = Some(Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 15)))
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1))
    expected = None
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31))
    expected = None
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15))
    expected = None
    assert(result == expected)

    result = input intersect Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15))
    expected = None
    assert(result == expected)
  }

  "compare" should "indicate the order of precedence of the start time of two Periods" in {
    val input1 = Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15))
    var input2 = Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15))
    assert(Period.compare(input1, input2) == 0)

    input2 = Period(functions.makeTS(2019, 12, 10), functions.makeTS(2019, 12, 15))
    assert(Period.compare(input1, input2) == -1)

    input2 = Period(functions.makeTS(2019, 11, 10), functions.makeTS(2019, 12, 15))
    assert(Period.compare(input1, input2) == 1)
  }
}
