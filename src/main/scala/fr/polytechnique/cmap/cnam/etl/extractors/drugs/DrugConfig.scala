package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.etl.extractors.ExtractorConfig

trait DrugClassConfig extends java.io.Serializable {
  val name: String
  val cip13Codes: Set[String]
  val pharmacologicalClasses: List[PharmacologicalClassConfig]
}

class DrugConfig(
  val level: DrugClassificationLevel, val families: List[DrugClassConfig]) extends ExtractorConfig with Serializable {
}
object DrugConfig {
  def apply(level: DrugClassificationLevel, families: List[DrugClassConfig]) : DrugConfig = new DrugConfig(level, families)
}

class PharmacologicalClassConfig(
  val name: String,
  val ATCCodes: List[String],
  val ATCExceptions: List[String] = List(),
  val CIPExceptions: List[String] = List()
) extends java.io.Serializable {

  def isCorrect(atc5code: String, cip13code: String): Boolean = {
    isCorrectATC(atc5code) && !isException(atc5code) && !isCIPException(cip13code)
  }

  def isCorrectATC(atc5code: String): Boolean = {
    compare(atc5code, ATCCodes)
  }

  def isException(atc5code: String): Boolean = {
    compare(atc5code, ATCExceptions)
  }

  def isCIPException(cip13code: String): Boolean = {
    CIPExceptions.exists(_.equals(cip13code))
  }

  def compare(atc5code: String, codeList: List[String]): Boolean = {
    codeList.exists{
      code =>
        if (code.endsWith("*"))
           atc5code.startsWith(code.dropRight(1))
        else
           code == atc5code
    }
  }
}
