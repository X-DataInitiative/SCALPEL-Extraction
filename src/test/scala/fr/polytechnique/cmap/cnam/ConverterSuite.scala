package fr.polytechnique.cmap.cnam

import org.scalatest._
import org.apache.parquet._
//.{Footer, ParquetFileReader, ParquetFileWriter}

/**
  * Created by burq on 25/06/16.
  */
class ConverterSuite extends FunSuite with GivenWhenThen{

  test("Read the CSV file") {
    Given("a CSV file")
    val srcFilePath: String = "../resources/IR_BEN_R.csv"
    val converter = new Converter(srcFilePath)

    When("I convert to parquet")
    val outputPath = converter.toParquet()


    Then("I should get a full CSV file")
    assert(outputPath == "toto")

  }

}
