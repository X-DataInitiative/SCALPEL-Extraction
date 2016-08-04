package fr.polytechnique.cmap.cnam.flattening

import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.FlatteningConfig._
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import JoinedTable._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}


class JoinedAggregatedTable(config: Config, sqlContext: SQLContext){
  import sqlContext.implicits._
 
  val name: String = config.name
  val joinKeys: List[String] = config.columns

  val mainTable: DataFrame = sqlContext
    .read
    .option("mergeSchema", "true")
    .parquet(path + "/" + config.mainTable)

  var isPartitioned: Boolean = false

  val years: Array[Int] = {
    if(mainTable.select($"key").distinct.cache.count==1) {
      isPartitioned = false
      mainTable.select(year($"FLX_TRT_DTD")).distinct.collect().map(_.getInt(0))
    } else {
      isPartitioned = true
      mainTable.select($"key").distinct.collect().map(_.getInt(0))
    }
  }

  val otherTables: Map[String, DataFrame] = config.tables.map(
    name =>
      name ->
        sqlContext
        .read
        .option("mergeSchema", "true")
        .parquet(path + "/" + name)
  ).toMap

 

  val otherTablesAggregated: Traversable[DataFrame] = otherTables.map(aggregate)

  val filterColumn: Column = if(isPartitioned){
    $"key"
  }else{
    year($"FLX_TRT_DTD")
  }

  val joinedDFPerYear: Map[Int,DataFrame] = years.map(joinDataFrames).toMap

 

  def joinDataFrames(year: Int): (Int, DataFrame) = {
    year ->
      otherTablesAggregated.map{
        _.filter(filterColumn === year).drop($"key")
      }.foldLeft(mainTable.filter(filterColumn === year))(joinFunction)
  }

  def joinFunction(accumulator: DataFrame, other: DataFrame): DataFrame = {
    val _joinKeys = joinKeys
    accumulator.join(other, _joinKeys, "left_outer")
  }

  def aggregate(keyValue: (String, DataFrame)): DataFrame = keyValue match{
    case (name: String, df: DataFrame) => {
      JoinedAggregatedTable.aggregateDataFrame(df,name,sqlContext,joinKeys)
    }
  }
  
  


}

object JoinedAggregatedTable {
  val path: String = FlatteningConfig.outputPath
  
  def aggregateDataFrame(data:DataFrame,aggregatedColumnName: String, sqlContext: SQLContext,_joinKeys:List[String] ): DataFrame={
	val aggregatedRDD=aggregateColumnsPerKey(data,sqlContext)
	var newColumnsArray=Seq[StructField]()
	var aggregatedColumnsArray=Seq[StructField]()
	val columns=data.schema.toVector
	for( i <- 0 to columns.length-1){
		if (_joinKeys contains columns(i).name) newColumnsArray=newColumnsArray:+columns(i)
		else aggregatedColumnsArray=aggregatedColumnsArray:+columns(i)
	}
	newColumnsArray=newColumnsArray:+StructField(aggregatedColumnName,ArrayType(StructType(aggregatedColumnsArray)))
	val newSchema=StructType(newColumnsArray)
	
	sqlContext.createDataFrame(aggregatedRDD,newSchema)
	
  }
  
  def aggregateColumnsPerKey(data:DataFrame, sqlContext: SQLContext, _joinKeys:List[String]): RDD[Row] ={
	val sc=sqlContext.sparkContext
    val rddData = data.rdd
	
	val schemaVector=data.schema.toVector
	var posJoinKeys=Array[Int]()
	for (i <-0 to _joinKeys.length-1){
		posJoinKeys=posJoinKeys:+fieldPos(schemaVector,_joinKeys(i))
	}
	val posJoinKeysBC=sc.broadcast(posJoinKeys)
	
	val initialVector = Seq[Any]()
	val addTwoVectors = (s: Seq[Any], v: Seq[Any]) => s ++ v
	val mergePartitions = (p1: Seq[Any], p2: Seq[Any]) => p1 ++ p2
	val newRDD=rddData.map{ x=>
		var key=Vector[Any]()
		var otherColumns=Seq[Any]()
		for( i <-0 to x.length-1){
			if( posJoinKeysBC.value contains i){
				key=key:+x(i)
			}
			else{
				otherColumns=otherColumns:+x(i)
			}
		}
		(key,Seq(Row(otherColumns:_*)))}.
		aggregateByKey(initialVector )(addTwoVectors,mergePartitions).
		map{x=>
			var keyColumns=Vector[Any](x._1:_*)
			keyColumns=keyColumns:+x._2.to[List]
			Row(keyColumns:_*)
		}
	newRDD
  
  }
  def fieldPos(columns:Vector[StructField],name:String) : Int ={
	for( i <- 0 to columns.length-1){
		if(columns(i).name==name){
			return i;
		}
	}
	return -1;
  }
}