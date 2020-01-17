// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.transformers.exposures

import me.danielpes.spark.datetime.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import fr.polytechnique.cmap.cnam.etl.datatypes._
import fr.polytechnique.cmap.cnam.etl.events.{Event, Exposure}
import fr.polytechnique.cmap.cnam.util.functions

class ExposureDurationSuite extends AnyFlatSpec {
  "+" should "return a RemainingPeriod of type Period indicating the addition of Periods" in {
    val input = ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)
    var result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 11, 5)),
      0L
    )
    var expected: RemainingPeriod[ExposureDuration] = RightRemainingPeriod(
      ExposureDuration("a", "b", Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)), 0L)
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 12, 1)),
        0L
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 11, 15)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
        0L
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 11, 5), functions.makeTS(2019, 12, 15)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 15)),
        0L
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 11, 15)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 10, 5), functions.makeTS(2019, 12, 1)),
        0L
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1)),
      0L
    )
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 11, 1)),
          0L
        )
      ),
      RightRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
          0L
        )
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31)),
      0L
    )
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 10, 1), functions.makeTS(2019, 10, 31)),
          0L
        )
      ),
      RightRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
          0L
        )
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)),
      0L
    )
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
          0L
        )
      ),
      RightRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 12, 5), functions.makeTS(2019, 12, 15)),
          0L
        )
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)),
      0L
    )
    expected = DisjointedRemainingPeriod(
      LeftRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
          0L
        )
      ),
      RightRemainingPeriod(
        ExposureDuration(
          "a",
          "b",
          Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)),
          0L
        )
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "d",
      "b",
      Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
        0L
      )
    )
    assert(result == expected)

    result = input + ExposureDuration(
      "a",
      "f",
      Period(functions.makeTS(2019, 12, 1), functions.makeTS(2019, 12, 15)),
      0L
    )
    expected = RightRemainingPeriod(
      ExposureDuration(
        "a",
        "b",
        Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
        0L
      )
    )
    assert(result == expected)

  }

  "PurchaseCountBased" should "set the End of the Exposure as being the start + purchase_count * purchase_duration " in {
    val input = ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
      45.days.totalMilliseconds
    )

    val expected: Event[Exposure] = Exposure(
      "a",
      "NA",
      "b",
      1D,
      functions.makeTS(2019, 11, 1),
      Some(functions.makeTS(2019, 12, 16))
    )

    val result = PurchaseCountBased(input)

    assert(expected == result)
  }

  "LatestPurchaseBased" should "set the End of the Exposure as being end of the latest Exposure" in {
    val input = ExposureDuration(
      "a",
      "b",
      Period(functions.makeTS(2019, 11, 1), functions.makeTS(2019, 12, 1)),
      45.days.totalMilliseconds
    )

    val expected: Event[Exposure] = Exposure(
      "a",
      "NA",
      "b",
      1D,
      functions.makeTS(2019, 11, 1),
      Some(functions.makeTS(2019, 12, 1))
    )

    val result = LatestPurchaseBased(input)

    assert(expected == result)
  }
}
