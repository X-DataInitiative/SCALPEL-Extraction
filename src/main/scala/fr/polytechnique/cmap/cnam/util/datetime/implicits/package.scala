package fr.polytechnique.cmap.cnam.util.datetime

// inspired by https://github.com/danielpes/spark-datetime-lite

package object implicits {

  implicit def toRichInt(value: Int): RichInt = new RichInt(value)
  implicit def toRichLong(value: Long): RichLong = new RichLong(value)
  implicit def toRichDate[A <: java.util.Date](date: A): RichDate[A] = new RichDate[A](date)
}
