package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.statistics.Comparator
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlatteningMain extends Main {

  def appName = "Flattening"

  def loadToParquet(): Unit = {
    val tables = FlatteningConfig.tablesConfig
      .map(
        config =>
          config.name -> new SingleTable(config, sqlContext)
      ).toMap

    tables.foreach{
      case(_, value) =>
        println("################################################################################")
        println("SINGLE WRITE " + value.tableName +
          " at year " + value. key +
          " format " + value.dateFormat)
        println("################################################################################")
        value.df.writeParquet(
          FlatteningConfig.outputPath + "/" + value.tableName + "/key=" + value.key
        )
        value.df.unpersist()
    }
  }

  def joinTables(): Unit = {
    val joinTables = FlatteningConfig.joinTablesConfig
      .map(
        config => {
          config.name -> new JoinedTable(config, sqlContext)
        }
      ).toMap

    joinTables.foreach{
      case(_, value) => {
        println("################################################################################")
        println("JOINING " + value.name )
        value.otherTables.keys.foreach(key => println("- " + key))
        value.joinedDFPerYear.foreach {
          case (year: Int, df: DataFrame) => {
            println("################################################################################")
            println("WRITING " + value.name + " at year " + year)
            df.writeParquet(
              FlatteningConfig.outputPath + "/joins/" + value.name + "/key=" + year
            )
            df.unpersist()
            println("DONE " + value.name + " at year " + year)
            println("################################################################################")
          }
        }
      }
    }
  }

  def validateJoinedTables() = {

//    val dcirFlatDF = sqlContext.read.parquet("/shared/burq/joins/DCIR")
    val dcirFlatDF = sqlContext.read.parquet("/shared/flat_data/joins/DCIR")

    val dcirKeyColumns = Seq(
    "DCT_ORD_NUM",
    "FLX_DIS_DTD",
    "FLX_EMT_NUM",
    "FLX_EMT_ORD",
    "FLX_EMT_TYP",
    "FLX_TRT_DTD",
    "ORG_CLE_NUM",
    "PRS_ORD_NUM",
    "REM_TYP_AFF"
    )

    val dcirMainTable = sqlContext.read.parquet("/shared/flat_data/ER_PRS_F")
    val ER_PHA_F = sqlContext.read.parquet("/shared/flat_data/ER_PHA_F")
    val ER_UCD_F = sqlContext.read.parquet("/shared/flat_data/ER_UCD_F")
    //val ER_CAM_F = sqlContext.read.parquet("/shared/burq/ER_CAM_F")

    val dcirSubTables: Map[String, DataFrame] = Map(
      "ER_PHA_F" -> ER_PHA_F,
      "ER_UCD_F" -> ER_UCD_F)

//    val expectedFlatDfColumns = {
//      dcirMainTable.columns ++
//      dcirSubTables
//        .map(subTable =>
//          subTable._2.columns
//            .map(
//              colName => {
//                if (dcirKeyColumns.contains(colName) || colName == "key")
//                  col(colName)
//                else
//                  col(subTable._1 + "." + colName).as(colName)
//              }
//            )).reduce(_ ++ _)
//    }.toSet.toSeq.sorted

//    val flatDFColumnsToSelect = {
//      dcirMainTable.columns.map(colName => col(colName)) ++
//      dcirSubTables
//        .map(subTable =>
//          subTable._2.columns
//            .map(
//              colName => {
//                if (dcirKeyColumns.contains(colName) || colName == "key")
//                  col(colName)
//                else
//                  col("`"+subTable._1 + "." + colName+"`").as(subTable._1 + "_" + colName)
//              }
//            )).reduce(_ ++ _)
//    }.toSet.toSeq

    val flatDFColumnsToSelect = {
      dcirFlatDF.columns
        .map(
          colName => {
            val splitted = colName.split("\\.")
            if (splitted.size == 2)
              col("`" + colName + "`").as(splitted(0) + "_" + splitted(1))
            else
              col(colName)
          }
        )
    }

    val subDfsWithAnnotatedColNames = {
//      Seq(dcirMainTable) ++
      dcirSubTables
          .map(subTable => subTable._2.select({
            subTable._2.columns
              .map(
                colName => {
                  if (dcirKeyColumns.contains(colName) || colName == "key")
                    col(colName)
                  else
                    col(colName).as(subTable._1 + "_" + colName)
                }
              )}:_*)) //.reduce(_ ++ _)
    }.toSeq //.toSet.toSeq.sorted

//    val flatDFResult = Comparator.unifyColNamesAndDescribeDistinct(dcirFlatDF.select(flatDFColumnsToSelect:_*))
    val chunckDfsResult = Comparator.unifyColNamesAndDescribeDistinct(dcirMainTable, subDfsWithAnnotatedColNames:_*)

//    flatDFResult.writeParquet("/shared/checkFlattening/DCIRFlat")
    chunckDfsResult.writeParquet("/user/kumar/checkFlattening/DCIRChuncks")
  }

  def main(args: Array[String]): Unit = {
    startContext()
//    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "104857600")
//    loadToParquet()
//    joinTables()
    validateJoinedTables()
  }
}