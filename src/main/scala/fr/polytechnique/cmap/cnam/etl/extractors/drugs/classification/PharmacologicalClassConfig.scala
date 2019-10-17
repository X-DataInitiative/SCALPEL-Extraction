// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification

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

  def compare(atc5code: String, codeList: List[String]): Boolean = {
    codeList.exists {
      code =>
        if (code.endsWith("*")) {
          atc5code.startsWith(code.dropRight(1))
        } else {
          code == atc5code
        }
    }
  }

  def isCIPException(cip13code: String): Boolean = {
    CIPExceptions.exists(_.equals(cip13code))
  }
}