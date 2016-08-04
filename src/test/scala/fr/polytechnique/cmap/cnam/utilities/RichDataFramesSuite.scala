package fr.polytechnique.cmap.cnam.utilities

import org.mockito._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._

/**
  * Created by burq on 05/07/16.
  */
class RichDataFramesSuite extends SharedContext {


  "===" should "return true" in {
    val sqltx = sqlContext
    import sqltx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,3)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(result)
  }

  it should "return false" in {
    val sqltx = sqlContext
    import sqltx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,4)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 === df2

    // Then
    assert(!result)
  }

  "storeParquet" should "call the write method" in {
    val sqltx = sqlContext
    import sqltx.implicits._
    // Given
    val df = sc.parallelize(Seq(1,2,3)).toDF("Interesting Column Name")
    val spyDF = Mockito.spy(df)

    // When
    spyDF.writeParquet("anyPath")

    // Then
    Mockito.verify(spyDF).write
  }

}
