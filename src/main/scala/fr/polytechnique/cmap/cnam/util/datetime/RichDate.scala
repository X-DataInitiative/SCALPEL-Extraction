package fr.polytechnique.cmap.cnam.util.datetime

import java.util.Calendar

/* inspired by https://github.com/danielpes/spark-datetime-lite */

private[datetime] class RichDate[A <: java.util.Date](val datetime: A) {

  def +(period: Period): A = {
    val c = Calendar.getInstance()
    c.setTime(datetime)
    c.add(Calendar.MONTH, period.totalMonths)
    val totalMillis = c.getTimeInMillis + period.totalMilliseconds

    datetime match {
      case _: java.sql.Date => new java.sql.Date(totalMillis).asInstanceOf[A]
      case _: java.sql.Timestamp => new java.sql.Timestamp(totalMillis).asInstanceOf[A]
    }
  }

  def -(p: Period): A = this + (-p)

  def between[B <: java.util.Date](lower: B, upper: B, includeBounds: Boolean = true): Boolean = {
    if (includeBounds) datetime.getTime >= lower.getTime && datetime.getTime <= upper.getTime
    else datetime.getTime > lower.getTime && datetime.getTime < upper.getTime
  }
}
