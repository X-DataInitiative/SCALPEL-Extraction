package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.Antidepresseurs

class PharmacologicalClassConfigSuite extends SharedContext{

  "isCorrect" should "return true when the atcCode is in the ocdeList and is not an exception and the CIP is not an exception" in {

    //Given
    val atcCode = "N06AX22"
    val drugConfigAntidepresseursAutres = Antidepresseurs.autres

    //When
    val result = drugConfigAntidepresseursAutres.isCorrect(atcCode, "")

    //Then
    assert(result)
  }

  "isCorrect" should "return false when the atcCode is not in the ocdeList" in {

    //Given
    val atcCode = "N06AY54"
    val drugConfigAntidepresseursAutres = Antidepresseurs.autres

    //When
    val result = drugConfigAntidepresseursAutres.isCorrect(atcCode, "")

    //Then
    assert(!result)
  }

  "isCorrect" should "return false when the cipCode is in the exception list" in {

    //Given
    val cipCode = "1265461"
    val atcCode = "123"
    val drugConfigTest = new PharmacologicalClassConfig("test", List(atcCode), List(), List(cipCode))

    //When
    val result = drugConfigTest.isCorrect(atcCode, cipCode)
    //Then
    assert(!result)
  }

  "isCorrectATC" should "return true when the atcCode is in the ocdeList" in {

    //Given
    val atcCode = "N06AX22"
    val drugConfigAntidepresseursAutres = Antidepresseurs.autres
    //When
    val result = drugConfigAntidepresseursAutres.isCorrectATC(atcCode)
    //Then
    assert(result)
  }


  "isException" should "return true when the atcCode is in the list exception" in {

    //Given
    val atcCode = "N06AA06"
    val drugConfigAntidepresseursAutres = Antidepresseurs.trycicliques
    //When
    val result = drugConfigAntidepresseursAutres.isException(atcCode)
    //Then
    assert(result)
  }

  "isCIPException" should "return false when the cip list exception is empty" in {
    //Given
    val atcCode = "1265461"
    val drugConfigAntidepresseursAutres = Antidepresseurs.trycicliques
    //When
    val result = drugConfigAntidepresseursAutres.isCIPException(atcCode)
    //Then
    assert(!result)
  }

  "isCIPException" should "return true when the cipCode is in cip list exception" in {
    //Given
    val cipCode = "1265461"
    val drugConfigTest = new PharmacologicalClassConfig("test", List("123"), List(), List(cipCode))

    //When
    val result = drugConfigTest.isCIPException(cipCode)
    //Then
    assert(result)
  }

  "compare" should "return true when the atcCode is in the codeList" in {

    //Given
    val atcCode = "N06AX22"
    val drugConfigAntidepresseursAutres = Antidepresseurs.autres
    //When
    val result = drugConfigAntidepresseursAutres.compare(atcCode, drugConfigAntidepresseursAutres.ATCCodes)
    //Then
    assert(result)
  }

  "compare" should "return true when the atcCode is in the codeList and the code list is defined by *" in {

    //Given
    val atcCode = "N06AG56"
    val drugConfigAntidepresseursimaoA = Antidepresseurs.imaoA
    //When
    val result = drugConfigAntidepresseursimaoA.compare(atcCode, drugConfigAntidepresseursimaoA.ATCCodes)
    //Then
    assert(result)
  }

  "compare" should "return false when the atcCode is not in the codeList" in {

    //Given
    val atcCode = "N06AY98"
    val drugConfigAntidepresseursAutres = Antidepresseurs.autres
    //When
    val result = drugConfigAntidepresseursAutres.compare(atcCode, drugConfigAntidepresseursAutres.ATCCodes)
    //Then
    assert(!result)
  }

}
