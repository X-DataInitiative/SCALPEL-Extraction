// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts

//ngapCoefficients should always be specified with the dot separation for float, as this is how they are coded in the snds.
// eg: "2.0" should be used instead of "2"
class NgapActClassConfig(
  val ngapKeyLetters: Seq[String],
  val ngapCoefficients: Seq[String]) extends Serializable

object NgapActClassConfig {
  def apply(ngapKeyLetters: Seq[String], ngapCoefficients: Seq[String]): NgapActClassConfig =
    new NgapActClassConfig(ngapKeyLetters,ngapCoefficients)
}

// If your Extractor add the Information from IR_NAT_V reference table use this.
class NgapWithNatClassConfig(
  override val ngapKeyLetters: Seq[String],
  override val ngapCoefficients: Seq[String],
  val ngapPrsNatRefs: Seq[String]) extends NgapActClassConfig(ngapKeyLetters, ngapCoefficients)
