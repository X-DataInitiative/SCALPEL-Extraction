package fr.polytechnique.cmap.cnam.util.datetime

import java.time.{LocalDate, LocalDateTime, LocalTime}

/* inspired by https://github.com/danielpes/spark-datetime-lite */

package object implicits {

  implicit def toRichInt(value: Int): RichInt = new RichInt(value)
  implicit def toRichLong(value: Long): RichLong = new RichLong(value)
  implicit def toRichDate[A <: java.util.Date](date: A): RichDate[A] = new RichDate[A](date)
  implicit def javaDateTimeToTimestamp(datetime: LocalDateTime): java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(datetime)
  }
  implicit def javaDateToTimestamp(date: LocalDate): java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(LocalDateTime.of(date, LocalTime.of(0, 0, 0)))
  }
}
