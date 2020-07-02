// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.level

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.events.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

class PharmacologicalSuite extends SharedContext {

  private val family = new DrugClassConfig {
    override val name: String = "mock"
    override val cip13Codes: Set[String] = Set("3400930820629")
    override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List(
      new PharmacologicalClassConfig(
        "mock",
        ATCCodes = List("mock")
      )
    )
  }

  "isInFamily" should "return false when the Row doesn't belong to the family" in {

    //Given
    val schema = StructType(StructField("ATC5", StringType) :: Nil)
    val inputArray = Array[Any]("no-shit-sherlock")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = PharmacologicalLevel.isInFamily(families, input)

    //Then
    assert(!result)
  }

  it should "return true when the Row belongs to the family" in {

    //Given
    val schema = StructType(StructField("ATC5", StringType) :: Nil)
    val inputArray = Array[Any]("mock")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = PharmacologicalLevel.isInFamily(families, input)

    //Then
    assert(result)
  }

  "getClassification" should "return a the Pharmacological class in Seq" in {

    //Given
    val schema = StructType(StructField("ATC5", StringType) :: Nil)
    val inputArray = Array[Any]("mock")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = PharmacologicalLevel.getClassification(families)(input)

    //Then
    assert(result == Seq("mock"))
  }

  it should "return a the CIP13 code in Seq when empty families are passed" in {

    //Given
    val schema = StructType(StructField("ATC5", StringType) :: StructField("CIP13", StringType) :: Nil)
    val inputArray = Array[Any]("mock", "test")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List.empty
    //When
    val result = PharmacologicalLevel.getClassification(families)(input)

    //Then
    assert(result == Seq("test"))
  }

}
