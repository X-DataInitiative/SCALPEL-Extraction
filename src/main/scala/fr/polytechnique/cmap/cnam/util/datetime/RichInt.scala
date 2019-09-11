package fr.polytechnique.cmap.cnam.util.datetime

/* inspired by https://github.com/danielpes/spark-datetime-lite */

private[datetime] class RichInt(val value: Int) extends AnyVal {

  def year: Period = this.years

  def years: Period = Period(years = value)

  def month: Period = this.months

  def months: Period = Period(months = value)

  def day: Period = this.days

  def days: Period = Period(days = value)

  def hour: Period = this.hours

  def hours: Period = Period(hours = value)

  def minute: Period = this.minutes

  def minutes: Period = Period(minutes = value)

  def second: Period = this.seconds

  def s: Period = this.seconds

  def seconds: Period = Period(seconds = value)
}
