package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActClassConfig

 object Ams extends NgapActClassConfig {
   val naturePrestation: Int = 3125
   val ngapCoefficients: Seq[String] = Seq(
     "9.5"
   )
}
