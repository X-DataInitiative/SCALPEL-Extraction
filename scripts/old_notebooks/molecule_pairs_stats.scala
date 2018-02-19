import org.apache.spark.sql.functions._
import org.apache.spark.sql._

val root_path = "/user/silva/mlpp_features/bladder_cancer/narrow/mlpp_features/30B-1L/"
val features_path = root_path + "parquet/SparseFeatures"
val outcomes_path = root_path + "csv/Outcomes.csv"
val output_path = "mlpp_stats/"

val spark = SparkSession.builder().getOrCreate()

val featuresDF = spark.read.parquet(features_path)
val outcomesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(outcomes_path)

def getMolPairs(l: List[String]): List[(String, String)]= {
  l.permutations.flatMap(_.combinations(2)).map(i => (i(0), i(1))).toSet.toList ++ l.zip(l)
}

def computePatientsPerMolecule(df: DataFrame): DataFrame = {
  df.groupBy("moleculeName").agg(countDistinct("patientID").as("numPatients")).orderBy("moleculeName")
}

def computeCancerHist(df: DataFrame): DataFrame = {
  import df.sqlContext.implicits._ 
  val maxBucket = outcomesDF.select("bucket").as[Int].reduce(math.max _)
  val rangeDF = spark.range(maxBucket+1).withColumn("count", lit(0)).withColumnRenamed("id", "bucket")
  df.groupBy("bucket").count.union(rangeDF).groupBy("bucket").max("count").orderBy("bucket")
}

case class Schema(patientID: String, moleculeName: String, bucketIndex: Int)

case class Result(both: Int, before: Int, sameBucket: Int, gap: Option[Int], onlyMol1: Int, mol1WithoutMol2: Int) {
//  def +(r: Result) = Result(
//    this.both + r.both, this.before + r.before, this.sameBucket + r.sameBucket, this.gap + r.gap,
//    this.onlyMol1 + r.onlyMol1, this.mol1WithoutMol2 + r.mol1WithoutMol2
//  )
}

def check(list: List[Schema], pair: (String, String)): Result = {
  val a1 = list.find(_.moleculeName == pair._1)
  val a2 = list.find(_.moleculeName == pair._2)
  
  if(a1.isEmpty) return Result(0, 0, 0, None, 0, 0)
  
  val onlyMol1 = if(list.toSet.size == 1) 1 else 0
  
  if(a2.isEmpty) {
    return Result(0, 0, 0, None, onlyMol1, 1)
  }
  
  val before = if (a1.get.bucketIndex < a2.get.bucketIndex) 1
  else 0
  
  val sameBucket = if (a1.get.bucketIndex == a2.get.bucketIndex) 1
  else 0

  val gap = if(a1.get.bucketIndex < a2.get.bucketIndex) Some(a2.get.bucketIndex - a1.get.bucketIndex)
  else None
  
  Result(1, before, sameBucket, gap, onlyMol1, 0)
}

def computePairStats(df: DataFrame, pairs: List[(String, String)]) = {
  
  import df.sqlContext.implicits._  
  val ds = df.groupBy("patientID", "moleculeName").agg(min("bucketIndex").cast("int").as("bucketIndex")).as[Schema]

  pairs.map(
    pair => {
      ds.groupByKey(_.patientID).mapGroups {
        (k, it) => check(it.toList, pair)
      }.agg(
        lit(pair._1).as("mol1"),
        lit(pair._2).as("mol2"),
        sum("both").as("both"),
        sum("before").as("before"),
        sum("sameBucket").as("same_bucket"),
        mean("gap").as("mean_gap"),
        min("gap").as("min_gap"),
        max("gap").as("max_gap"),
        sum("onlyMol1").as("only_mol1"),
        sum("mol1WithoutMol2").as("mol1_without_mol2")
      )
    }
  ).reduce(_.union(_)).orderBy("mol1", "mol2")
}

def writeDF(df: DataFrame, path_suffix: String): Unit = {
  df.coalesce(1).write.option("header", "true").csv(output_path + path_suffix)
}

val molecules = List("INSULINE", "METFORMINE", "OTHER", "PIOGLITAZONE", "ROSIGLITAZONE", "SULFONYLUREA")
val molPairs = getMolPairs(molecules)

val patientsPerMolecule = computePatientsPerMolecule(featuresDF)
val cancerHist = computeCancerHist(outcomesDF)
val pairStats = computePairStats(featuresDF, molPairs)

