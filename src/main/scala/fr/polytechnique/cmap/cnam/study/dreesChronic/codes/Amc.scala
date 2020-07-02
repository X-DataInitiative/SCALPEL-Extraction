package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts.NgapWithNatClassConfig

object Amc extends NgapWithNatClassConfig(
  ngapKeyLetters = Seq("AMC"),
  ngapCoefficients = Seq("20.0", "28.0"),
  ngapPrsNatRefs = Seq("3121")
)
