package fr.polytechnique.cmap.cnam.etl.extractors.drugs.level

import org.mockito.Mockito
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

class TherapeuticSuite extends SharedContext{

  private val family = new DrugClassConfig {
    override val name: String = "mock"
    override val cip13Codes: Set[String] = Set("3400930820629")
    override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List(Mockito
      .mock(classOf[PharmacologicalClassConfig]))
  }

  "isInFamily" should "return false when the Row doesn't belong to the family" in {

    //Given
    val schema = StructType(StructField("CIP13", StringType) :: Nil)
    val inputArray = Array[Any]("3400934207839654")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = TherapeuticLevel.isInFamily(families, input)

    //Then
    assert(!result)
  }

  "isInFamily" should "return true when the Row belongs to the family" in {

    //Given
    val schema = StructType(StructField("CIP13", StringType) :: Nil)
    val inputArray = Array[Any]("3400930820629")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = TherapeuticLevel.isInFamily(families, input)

    //Then
    assert(result)
  }

  "getClassification" should "return a the Therapeutical class in a Seq" in {

    //Given
    val schema = StructType(StructField("CIP13", StringType) :: Nil)
    val inputArray = Array[Any]("3400930820629")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List(family)
    //When
    val result = TherapeuticLevel.getClassification(families)(input)

    //Then
    assert(result == Seq("mock"))
  }

  "getClassification" should "return a the CIP13 code in Seq when families is empty" in {

    //Given
    val schema = StructType(StructField("CIP13", StringType) :: Nil)
    val inputArray = Array[Any]("3400930820629")
    val input = new GenericRowWithSchema(inputArray, schema)
    val families = List.empty
    //When
    val result = TherapeuticLevel.getClassification(families)(input)

    //Then
    assert(result == Seq("3400930820629"))
  }

}
