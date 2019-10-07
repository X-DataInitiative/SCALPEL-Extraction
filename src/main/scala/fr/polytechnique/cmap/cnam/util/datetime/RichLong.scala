// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.util.datetime

/* inspired by https://github.com/danielpes/spark-datetime-lite */

private[datetime] class RichLong(val value: Long) extends AnyVal {

  def millisecond: Period = this.milliseconds

  def milliseconds: Period = Period(milliseconds = value)

  def ms: Period = this.milliseconds
}
