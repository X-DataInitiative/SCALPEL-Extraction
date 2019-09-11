package fr.polytechnique.cmap.cnam.util.datetime

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime}

/* inspired by https://github.com/danielpes/spark-datetime-lite */

package object implicits {

  implicit def toRichInt(value: Int): RichInt = new RichInt(value)

  implicit def toRichLong(value: Long): RichLong = new RichLong(value)

  implicit def toRichDate[A <: java.util.Date](date: A): RichDate[A] = new RichDate[A](date)

  implicit def javaDateTimeToTimestamp(datetime: LocalDateTime): java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(datetime)
  }

  implicit def javaDateToTimestamp(date: LocalDate): java.sql.Timestamp = {
    java.sql.Timestamp.valueOf(date.atStartOfDay)
  }

  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }
}
