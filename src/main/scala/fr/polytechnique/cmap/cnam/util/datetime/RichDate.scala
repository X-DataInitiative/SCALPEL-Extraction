package fr.polytechnique.cmap.cnam.util.datetime

import java.util.Calendar

/* inspired by https://github.com/danielpes/spark-datetime-lite */

private[datetime] class RichDate[A <: java.util.Date](val datetime: A) {

  def -(p: Period): A = this + (-p)

  def +(period: Period): A = {
    val c = getCalendar
    c.add(Calendar.MONTH, period.totalMonths)
    val totalMillis = c.getTimeInMillis + period.totalMilliseconds

    datetime match {
      case _: java.sql.Date => new java.sql.Date(totalMillis).asInstanceOf[A]
      case _: java.sql.Timestamp => new java.sql.Timestamp(totalMillis).asInstanceOf[A]
    }
  }

  private def getCalendar: Calendar = {
    val c = Calendar.getInstance()
    c.setTime(datetime)
    c
  }

  def between[B <: java.util.Date](lower: B, upper: B, includeBounds: Boolean = true): Boolean = {
    if (includeBounds) {
      datetime.getTime >= lower.getTime && datetime.getTime <= upper.getTime
    } else {
      datetime.getTime > lower.getTime && datetime.getTime < upper.getTime
    }
  }

  def toTimestamp: java.sql.Timestamp = new java.sql.Timestamp(datetime.getTime)

  def toDate: java.sql.Date = {
    // We need to reset the time, because java.sql.Date contains time information internally.
    val c = getCalendar
    c.set(Calendar.MILLISECOND, 0)
    c.set(Calendar.SECOND, 0)
    c.set(Calendar.MINUTE, 0)
    c.set(Calendar.HOUR_OF_DAY, 0)
    new java.sql.Date(c.getTimeInMillis)
  }
}
