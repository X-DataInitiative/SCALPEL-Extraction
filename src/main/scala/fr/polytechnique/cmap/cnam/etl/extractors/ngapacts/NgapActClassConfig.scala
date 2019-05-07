package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

trait NgapActClassConfig extends java.io.Serializable {
    """
      ngapCoefficients should always be specified with the dot separation for float, as this is how they are coded in the snds.
      eg: "2.0" shoudl be used instead of "2"
    """.stripMargin
    //val name: String
    val naturePrestation: Int
    val ngapCoefficients: Seq[String]
}
