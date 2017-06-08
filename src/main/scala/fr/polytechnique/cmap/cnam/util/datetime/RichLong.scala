package fr.polytechnique.cmap.cnam.util.datetime

// inspired by https://github.com/danielpes/spark-datetime-lite

private[datetime] class RichLong(val value: Long) extends AnyVal {

  def milliseconds: Period = Period(milliseconds = value)
  def millisecond: Period = this.milliseconds
  def ms: Period = this.milliseconds
}
