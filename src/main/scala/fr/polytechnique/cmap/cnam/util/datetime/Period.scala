package fr.polytechnique.cmap.cnam.util.datetime

import scala.collection.immutable.ListMap

// inspired by https://github.com/danielpes/spark-datetime-lite

private[datetime] class Period(
    val totalMonths: Int = 0,
    val totalMilliseconds: Long = 0) extends Serializable {

  def unary_-(): Period = new Period(-totalMonths, -totalMilliseconds)

  def +(other: Period): Period = {
    new Period(totalMonths + other.totalMonths, totalMilliseconds + other.totalMilliseconds)
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
    msCounts.foldLeft((initialMap, totalMilliseconds)) {
      case ((newMap, rest), (unitName, msPerUnit)) =>
        (newMap + (unitName -> rest / msPerUnit), rest % msPerUnit)
    }._1
  }

  override def toString: String = {
    val stringList = this.toMap.collect {
      case (k, v) if v == 1 => s"$v ${k.dropRight(1)}"
      case (k, v) if v != 0 => s"$v $k"
    }
    if (stringList.isEmpty) "Empty Period"
    else stringList.mkString(", ")
  }
}

object Period {

  private val MS_PER_SEC: Long =  1000L
  private val MS_PER_MIN: Long =  MS_PER_SEC * 60L
  private val MS_PER_HOUR: Long = MS_PER_MIN * 60L
  private val MS_PER_DAY: Long =  MS_PER_HOUR * 24L

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
        hours   * MS_PER_HOUR +
        days    * MS_PER_DAY

    new Period(totalMonths, totalMilliseconds)
  }
}
