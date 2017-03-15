package fr.polytechnique.cmap.cnam.flattening

import java.io.File
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField}
import org.mockito.Mockito._
import com.typesafe.config.{Config, ConfigFactory}
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.FlatteningConfig

/**
  * Created by burq on 11/07/16.
  */
class SingleTableSuite extends SharedContext {
  import SingleTable._

  "df" should "return the IR_BEN_R dataframe" in {
    // Given
    val tableName: String = "IR_BEN_R"
    val config: Config = FlatteningConfig.getTableConfig(tableName)
    val expected: DataFrame = sqlContext.read.load("src/test/resources/expected/IR_BEN_R.parquet")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    assertDFs(result, expected)
  }

  it should "return the ER_PRS_F dataframe" in {
    // Given
    val tableName: String = "ER_PRS_F"
    val config: Config = FlatteningConfig.getTableConfig(tableName)
    val expected = sqlContext.read.load("src/test/resources/expected/ER_PRS_F.parquet")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    assertDFs(result, expected)
  }

  it should "return the ER_PHA_F dataframe" in {
    // Given
    val tableName: String = "ER_PHA_F"
    val config: Config = FlatteningConfig.getTableConfig(tableName)
    val expected = sqlContext.read.load("src/test/resources/expected/ER_PHA_F.parquet")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    assertDFs(result, expected)
  }

  it should "return the ER_UCD_F dataframe" in {
    // Given
    val tableName: String = "ER_UCD_F"
    val config: Config = FlatteningConfig.getTableConfig(tableName)
    val expected = sqlContext.read.load("src/test/resources/expected/ER_UCD_F.parquet")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    assertDFs(result, expected)
  }

  it should "return the IR_IMB_R dataframe" in {
    // Given
    val tableName: String = "IR_IMB_R"
    val config: Config = FlatteningConfig.getTableConfig(tableName)
    val expected = sqlContext.read.load("src/test/resources/expected/IR_IMB_R.parquet")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    assertDFs(result, expected)
  }

  it should "fail when config is wrong" in {
    // Given
    FlatteningConfig.addTable(ConfigFactory.parseFile(new File("src/test/resources/fake.conf")))
    val config = FlatteningConfig.getTableConfig("IR_BEN_Fake")

    // When
    val result = new SingleTable(config, sqlContext).df

    // Then
    intercept[org.apache.spark.SparkException] {
      result.show
    }
  }

  "toStructField" should "return NUM_ENQ StructField (nominal case)" in {
    // Given
    val mockConfig: Config = mock(classOf[Config])
    when(mockConfig.getString("name")).thenReturn("NUM_ENQ")
    when(mockConfig.getString("type")).thenReturn("StringType")

    val expectedName = "NUM_ENQ"
    val expectedType = StringType.simpleString

    // When
    val result: StructField = SingleTable.toStructField(mockConfig)

    // Then
    assert(result.dataType.simpleString == expectedType)
    assert(result.name == expectedName)
  }

  "getSchema" should "return the right StructType" in {
    // Given
    val config = ConfigFactory.parseString("fields = [\n" +
      "{name: toto, type: IntegerType}\n" +
      "{name: tata, type: DateType}\n" +
    "]")
    val expected = "struct<toto:int,tata:date>"

    // When
    val result = getSchema(config)

    // Then
    assert(result.simpleString == expected)
  }

}
