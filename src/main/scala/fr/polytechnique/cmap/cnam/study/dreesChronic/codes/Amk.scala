package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActClassConfig

 object Amk extends NgapActClassConfig {
   val naturePrestation: Int = 3122
   val ngapCoefficients: Seq[String] = Seq(
     "2.0",
     "8.0",
     "20.0",
     "28.0"
   )
}
