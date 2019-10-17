// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.datetime

import scala.collection.immutable.ListMap

/* inspired by https://github.com/danielpes/spark-datetime-lite */

private[datetime] class Period(
  val totalMonths: Int = 0,
  val totalMilliseconds: Long = 0)
  extends Serializable {

  def years: Int = this.toMap.apply("years").toInt

  def months: Int = this.toMap.apply("months").toInt

  def days: Int = this.toMap.apply("days").toInt

  def hours: Int = this.toMap.apply("hours").toInt

  def minutes: Int = this.toMap.apply("minutes").toInt

  def seconds: Int = this.toMap.apply("seconds").toInt

  def milliseconds: Long = this.toMap.apply("milliseconds")

  def unary_-(): Period = new Period(-totalMonths, -totalMilliseconds)

  def +(other: Period): Period = {
    new Period(totalMonths + other.totalMonths, totalMilliseconds + other.totalMilliseconds)
  }

  override def equals(that: Any): Boolean = that match {
    case that: Period => that.canEqual(this) &&
      this.totalMonths == that.totalMonths &&
      this.totalMilliseconds == that.totalMilliseconds
    case _ => false
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[Period]

  override def toString: String = {
    val stringList = this.toMap.collect {
      case (k, v) if v == 1 => s"$v ${k.dropRight(1)}"
      case (k, v) if v != 0 => s"$v $k"
    }
    if (stringList.isEmpty) "Empty Period" else stringList.mkString(", ")
  }

  def toMap: ListMap[String, Long] = {

    val msCounts: List[(String, Long)] = List(
      ("days", Period.MS_PER_DAY),
      ("hours", Period.MS_PER_HOUR),
      ("minutes", Period.MS_PER_MIN),
      ("seconds", Period.MS_PER_SEC),
      ("milliseconds", 1L)
    )
    val initialMap = ListMap(
      "years" -> totalMonths / 12L,
      "months" -> totalMonths % 12L
    )
    val (finalMap, _) = msCounts.foldLeft((initialMap, this.totalMilliseconds)) {
      case ((newMap, remainder), (unitName, msPerUnit)) =>
        (newMap + (unitName -> remainder / msPerUnit), remainder % msPerUnit)
    }

    finalMap
  }
}

object Period {

  private val MS_PER_SEC: Long = 1000L
  private val MS_PER_MIN: Long = MS_PER_SEC * 60L
  private val MS_PER_HOUR: Long = MS_PER_MIN * 60L
  private val MS_PER_DAY: Long = MS_PER_HOUR * 24L

  def apply(
    years: Int = 0,
    months: Int = 0,
    days: Int = 0,
    hours: Int = 0,
    minutes: Int = 0,
    seconds: Int = 0,
    milliseconds: Long = 0): Period = {

    val totalMonths = months + (12 * years)
    val totalMilliseconds = milliseconds +
      seconds * MS_PER_SEC +
      minutes * MS_PER_MIN +
      hours * MS_PER_HOUR +
      days * MS_PER_DAY

    new Period(totalMonths, totalMilliseconds)
  }
}
