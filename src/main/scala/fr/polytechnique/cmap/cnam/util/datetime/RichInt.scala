package fr.polytechnique.cmap.cnam.util.datetime

// inspired by https://github.com/danielpes/spark-datetime-lite

private[datetime] class RichInt(val value: Int) extends AnyVal {

  def years: Period = Period(years = value)
  def year: Period = this.years

  def months: Period = Period(months = value)
  def month: Period = this.months

  def days: Period = Period(days = value)
  def day: Period = this.days

  def hours: Period = Period(hours = value)
  def hour: Period = this.hours

  def minutes: Period = Period(minutes = value)
  def minute: Period = this.minutes

  def seconds: Period = Period(seconds = value)
  def second: Period = this.seconds
  def s: Period = this.seconds
}
