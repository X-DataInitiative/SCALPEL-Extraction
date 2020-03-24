package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

trait NgapActClassConfig extends Serializable {
  """
      ngapCoefficients should always be specified with the dot separation for float, as this is how they are coded in the snds.
      eg: "2.0" should be used instead of "2"
    """.stripMargin
  //val name: String
  val ngapKeyLetters: Seq[String]
  val ngapCoefficients: Seq[String]
  val ngapPrsNatRefs: Seq[String] = Seq()
}
