package fr.polytechnique.cmap.cnam.util.datetime

import org.scalatest.FlatSpec

class RichIntSuite extends FlatSpec {

  private val one = new RichInt(1)
  private val five = new RichInt(5)

  "year/years" should "create a corresponding Period instance" in {
    assert(one.year == Period(years = 1))
    assert(five.years == Period(years = 5))
  }
  "month/months" should "create a corresponding Period instance" in {
    assert(one.month == Period(months = 1))
    assert(five.months == Period(months = 5))
  }
  "day/days" should "create a corresponding Period instance" in {
    assert(one.day == Period(days = 1))
    assert(five.days == Period(days = 5))
  }
  "hour/hours" should "create a corresponding Period instance" in {
    assert(one.hour == Period(hours = 1))
    assert(five.hours == Period(hours = 5))
  }
  "minute/minutes" should "create a corresponding Period instance" in {
    assert(one.minute == Period(minutes = 1))
    assert(five.minutes == Period(minutes = 5))
  }
  "second/seconds" should "create a corresponding Period instance" in {
    assert(one.s == Period(seconds = 1))
    assert(one.second == Period(seconds = 1))
    assert(five.seconds == Period(seconds = 5))
  }
}
