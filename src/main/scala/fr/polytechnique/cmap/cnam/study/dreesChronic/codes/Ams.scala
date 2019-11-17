package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.ngapacts.NgapActClassConfig

// prs_nat_ref == 3125
 object Ams extends NgapActClassConfig {
   val ngapKeyLetters: Seq[String] = Seq("AMS")
   val ngapCoefficients: Seq[String] = Seq(
     "9.5"
   )
}
