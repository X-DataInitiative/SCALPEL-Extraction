package fr.polytechnique.cmap.cnam.flattening
import java.sql.Timestamp
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames
import fr.polytechnique.cmap.cnam.utilities.functions.makeTS
import org.scalatest.GivenWhenThen
import org.scalatest.FunSpec

/**
  * Created by admindses on 06/01/2017.
  */
class SASLoaderSuite extends SharedContext with GivenWhenThen{

  "run" should "read sas files and return correct converted csv files" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val configPath = "src/test/resources/config/sas.conf"

    val expected = Seq(
      (877:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,3:Double,1:Double,"01M311654","01M311654"),
      (1122:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,12:Double,1:Double,"01M311654","01M311654"),
      (1180:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,1:Double,1:Double,"01M311654","01M311654"),
      (1429:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,1:Double,1:Double,"01M311654","01M311654"),
      (1434:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,2:Double,1:Double,"01M311654","01M311654"),
      (1443:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,1:Double,1:Double,"01M311654","01M311654"),
      (1454:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,1:Double,1:Double,"01M311654","01M311654"),
      (1455:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,2:Double,1:Double,"01M311654","01M311654"),
      (1455:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,3:Double,1:Double,"01M311654","01M311654"),
      (1461:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,7:Double,1:Double,"01M311654","01M311654"),
      (1461:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,19),"R0569",null:String,"99","SVI_RPPS","01","IR-PRN",4:Double,8:Double,1:Double,"01M311654","01M311654"),
      (1530:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,2),"I0706",null:String,null:String,"ANO IRIS",null:String,"IR-PRS-IRI",1:Double,6:Double,1:Double,"01C821000","01C821000"),
      (1790:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,2),"I4012",null:String,null:String,"ANO IRIS",null:String,"IR-PRS-IRI",1:Double,1:Double,1:Double,"01C091000","01C091000"),
      (1969:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,2),"I0405",null:String,null:String,"ANO IRIS",null:String,"IR-PRS-IRI",1:Double,1:Double,1:Double,"01C821000","01C821000"),
      (1969:Double,makeTS(2014,7,1),2:Double,0:Double,1:Double,makeTS(2014,6,2),"I0405",null:String,null:String,"ANO IRIS",null:String,"IR-PRS-IRI",1:Double,3:Double,1:Double,"01C821000","01C821000")
    ).toDF("DCT_ORD_NUM","FLX_DIS_DTD","FLX_EMT_NUM","FLX_EMT_ORD","FLX_EMT_TYP","FLX_TRT_DTD","PRS_ANO_COD","PRS_ANO_COM","PRS_ANO_DEF","PRS_ANO_DON","PRS_ANO_ORI",
      "PRS_ENT_COD","PRS_ENT_NUM","PRS_ORD_NUM","REM_TYP_AFF","ORG_CLE_NEW","ORG_CLE_NUM")

    //When
    val rs  = SASLoader.ComputeDF(sqlContext,Map("conf" -> configPath,"outputFormat" -> "CSV"))(0)

    //Then
    assert(rs.count() == 100)

    val orderedDf = rs.orderBy("DCT_ORD_NUM","PRS_ORD_NUM").limit(15)
    import RichDataFrames._
    orderedDf.show()

    //assertion for dataframe equality (only first 15 lines))
    assert(orderedDf  === expected)

    rs.printSchema()
    val columnTypes = rs.dtypes
    //assertions for columns data types
    assert(columnTypes(0)._2 == "DoubleType")
    assert(columnTypes(1)._2 == "TimestampType")
    assert(columnTypes(2)._2 == "DoubleType")
    assert(columnTypes(3)._2 == "DoubleType")
    assert(columnTypes(4)._2 == "DoubleType")
    assert(columnTypes(5)._2 == "TimestampType")
    assert(columnTypes(6)._2 == "StringType")



  }



  "run" should "read sas files and return correct converted parquet files" in {
    val sqlCtx = sqlContext
    val configPath = "src/test/resources/config/sas.conf"
    val rs  = SASLoader.ComputeDF(sqlContext,Map("conf" -> configPath,"outputFormat" -> "Parquet"))(0)
    rs.printSchema()
    assert(rs.count() == 100)
  }



}
