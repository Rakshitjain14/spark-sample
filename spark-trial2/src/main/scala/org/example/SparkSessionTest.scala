package org.example
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{col, collect_set, lit, max, monotonically_increasing_id, row_number, trim}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.relaxng.datatype.Datatype

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.{Source, Codec => ScalaCodec}
import scala.util.control.Breaks.{break, breakable}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
object SparkSessionTest extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  println("First SparkContext:")
  println("APP Name :"+spark.sparkContext.appName);
  println("Deploy Mode :"+spark.sparkContext.deployMode);
  println("Master :"+spark.sparkContext.master);
  import spark.implicits._
  spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "\u0002")
  val lines:RDD[String] = spark.sparkContext.textFile("/Users/rakshitjain/Downloads/a_parquet_test/201912150120338835/a_parquet_test_20191215120338835.dat.gz")
  val lines2:RDD[String] = spark.sparkContext.textFile("/Users/rakshitjain/Downloads/a_parquet_test/201912151020338850/a_parquet_test_20191215120338850.dat.gz")
//"/Users/rakshitjain/Downloads/a_parquet_test/201912150120338840/a_parquet_test_20191215120338840.dat.gz," +
//  var data = lines.map(line => line.split("\u0001").map(elem => elem.trim))
//  var data2= data.map(x=>x.length).distinct().count()
////    var data3=data2.distinct()
//    //println("Using distinct "+ data2.toDF().collectAsList())
//    println("length of distinct array " + data2)
//private final val hvrCapTimestampCol = "hvr_cap_timestamp"
//  val x=1;
//  println("value is : "+ (x+1))

//  var schema:List[String]=List("timestamp", "optype", "hvr_integ_timestamp", "test_id", "hvr_cap_timestamp", "comments","just_number","just_number2")
//  println("schema is " + schema.toSet)
  val simpleSchema = StructType(Array(
    StructField("timestamp",TimestampType,true),
    StructField("optype",IntegerType,true),
    StructField("hvr_integ_timestamp",LongType,true),
    StructField("test_id", IntegerType, true),
    StructField("hvr_cap_timestamp", LongType, true),
    StructField("comments", StringType, true),
    StructField("just_number", IntegerType, true),
    StructField("Just_number2", IntegerType, true)
  ))

  //val lines2:RDD[String] = spark.sparkContext.textFile("/Users/rakshitjain/Downloads/a_parquet_test/201912151020338850/a_parquet_test_20191215120338850.dat.gz")
  //val lines3:RDD[String] = spark.sparkContext.textFile("/Users/rakshitjain/Downloads/a_parquet_test/201912151020338850/a_parquet_test_20191215120338850.dat.gz")

  var df1=convertodf(lines)
  //df1=df1.withColumn("test_id", col("test_id").cast(IntegerType))
  //println("df1.schema: "+df1.schema)
  var df2=convertodf(lines2)
  //var df4=convertodf(lines3)
  //df2=df2.withColumn("test_id", col("test_id").cast(IntegerType))
  //println("df2.schema: "+df2.columns.toSet)
//var test_id="2"
//  var hvr_cap_timestamp="20190601060000"
//  var df3=df1.filter("optype=" + test_id )
  //df1=df1.select("test_id","optype")
 // df1=df1.select("test_id" , lit("123").as("column"))
  df1.show()
  //df2.show()

  //println("column mismatch: "+ checkColumns5(df1,schema.toArray))
  //println("column mismatch2: "+ getColumnDifference(df1,schema.toArray))
  //println("column mismatch3: "+ getColumnDifference(schema.toArray,df1))
  //df2=df2.withColumn("just_number2", col("just_number2").cast(IntegerType))
  //println(df2.schema)
  var df3 = mergedf(df2,df1,simpleSchema)
  //simpleSchema.foreach(sf => if (df3.columns.contains(sf.name)) df3 = df3.withColumn(sf.name, col(sf.name).cast(sf.dataType)))
 println("df3.schema:"+df3.schema)
 df3.show()


  // processBasedOnOpType("abc",df3,"xyz",false)


//  var df3=mergedf(df1,df2)
//  println("df3 SCHEMA " + df3.schema)
//  df3=df3.withColumn("just_number2", col("just_number2").cast(IntegerType))
//  println("df3 SCHEMA 22" + df3.schema)
//  println("df3 is: \n")
//  df3.show()

//  val merged_cols = df1.columns.toSet ++ df2.columns.toSet
//  //val req=Array.concat(df1.columns,df2.columns)
//  val req=df1.columns.union(df2.columns).distinct.toSeq
//  println(merged_cols.toList)
//  println(req)
 /** println("df1 is: \n")
  df1.show()
  println("df2 is: \n")
  df2.show()
  var df3=mergedf(df1,df2)
  println("df3 is: \n")
  df3.show()
  val listcolumnchange=ListBuffer[String]()
  listcolumnchange.append("just_number2")
  //listcolumnchange.append("test_id")
  var condition2:String=null

  listcolumnchange.foreach(x=>{
    if(condition2==null)
    condition2=x + " == '$null$'"
    else
      condition2=condition2+" and " + x + " == '$null$'"


  })
  println("condition2 is :" + condition2)
  var condition3="!(" + condition2 + ")"

  val s2 = "just_number2 == '$null$' and test_id == 600 "
  val s = "Just_Number2"
  val condition = col(s) === ("$null$")
  var df4=df3.filter(condition3)
  //var df5=df3.filter('just_number2 === "$null$").drop(s)
  var df5=df3.filter(condition2)
  df5=df5.drop(listcolumnchange:_*)
  println("df4 is: \n")
  df4.show()
  println("df5 is: \n")
  df5.show()
*/
//  var df = spark.read.option("mergeSchema", "true").parquet("/Users/rakshitjain/Downloads/a_parquet_test/2021-08-13_13.05.30")
//  df.show()
//  val s = "just_number2"
//  val condition = col(s) === (null)
//  var df1=df.filter(condition)
//  df1.show()

  //logger.info("lines count is : " + lines.count() )
//  lines.take(50).foreach(println)

  //var data = lines.map(line => line.split("\u0001").map(elem => elem.trim))

//  var data2= data.map(x=>x.length)
//  var data3=data2.distinct()
//  println("Using distinct "+ data3.toDF().collectAsList())
//
//  var lst=data2.toDF().collectAsList();
//
//  var i =0;
//  var head = lst.get(0).getInt(0);
//
//  var schemaChangeindex = ListBuffer[Int]()
//  schemaChangeindex.append(head)
//  for(i <-1  until lst.size()) {
//  {
//    if(lst.get(i).getInt(0)!=head)
//    {
//      schemaChangeindex.append(lst.get(i).getInt(0))
//      head=lst.get(i).getInt(0)
//    }
//  }
//
//  }
//  println("Schemachangeindex:"+schemaChangeindex)
  //val header:Array[String] = data.first()
  //data = data.filter(row => !row.isEmpty && !(row sameElements header))
  //var df = data.toDF("arr").select(header.indices.map(i => col("arr")(i).alias(header(i).toLowerCase)): _*)
  //df.dropDuplicates()
  //df.show()


//  val data = Seq(("i20", "Hyundai"), ("Breza", "Maruti"), ("City", "Honda"))
//  val df3 = spark.createDataFrame(data).toDF("car_name","car_company")
//
//  val data2 = Seq(("Swift", "Maruti","7L"), ("Creta", "Hyundai","13L"), ("Amaze", "Honda","11L"))
//  val df2 = spark.createDataFrame(data2).toDF("car_name","car_company","cost")
////  val columns=df.columns.toSeq
//  //println(df.columns.toSet)
////  val finaldf = df.unionByName(df2.selectExpr("car_name","car_company"))
////  finaldf.show()
//
//  val data3 = Seq(("James","Smith","USA","CA"),
//                  ("Michael","Rose","USA","NY"),
//                  ("Robert","Williams","USA","CA"),
//                  ("Maria","Jones","USA","FL")
//  )
//  val columns = Seq("firstname","lastname","country","state")
//  import spark.implicits._
//  val df = data3.toDF(columns:_*)
//  df.show()
//  val listCols= List("lastname","country")
//  df.select(listCols.map(m=>col(m)):_*).show()
  //df.select(df.columns.slice(0,3).map(m=>col(m)):_*).show()
//  import spark.implicits._
  //df3.unionByName(df2.select(df3.columns.map(x=>col(x)):_*)).show()





//  import spark.implicits._
//  val simpleData = Seq(("James", "Sales", 3000),
//    ("Michael", "Sales", 4600),
//    ("Robert", "Sales", 4100),
//    ("Maria", "Finance", 3000),
//    ("James", "Sales", 3000),
//    ("Scott", "Finance", 3300),
//    ("Jen", "Finance", 3900),
//    ("Jeff", "Marketing", 3000),
//    ("Kumar", "Marketing", 2000),
//    ("Saif", "Sales", 4100)
//  )
//  var df = simpleData.toDF("employee_name", "department", "salary")
//  df.show()
//  df = df.repartition(df.col("department"))
//  df.sortWithinPartitions(df.col("salary")).show()
//  val windowSpec  = Window.partitionBy("department")
//  df.withColumn("max_salary",functions.max("salary").over(windowSpec))
//    .withColumn("values",functions.collect_set("salary").over(windowSpec))
//    .filter(col("max_salary")===col("salary"))
//    .show()
//
//
//
//  def expr(dfCols: Set[String], superDF: DataFrame): List[Column] = {
//    superDF.schema.toList.map {
//      case x if dfCols.contains(x.name) => col(x.name).cast(x.dataType)
//      case x => lit(null).as(x.name).cast(x.dataType)
//    }
//  }
//

  def convertodf(lines:RDD[String]): DataFrame =
  {
    var data = lines.map(line => line.split("\u0001").map(elem => elem.trim))
    val header:Array[String] = data.first()
    data = data.filter(row => !row.isEmpty && !(row sameElements header))
    val df = data.toDF("arr").select(header.indices.map(i => col("arr")(i).alias(header(i).toLowerCase)): _*)
     df

  }
  def mergedf(df1:DataFrame,df2:DataFrame,simpleschema:StructType): DataFrame = {
    val emp_data1Cols = df1.columns.toSeq
    val emp_data2Cols = df2.columns.toSeq
    //val requiredColumns = emp_data1Cols ++ emp_data2Cols // union
    val requiredColumns=df1.columns.union(df2.columns).distinct.toSeq


    var mergeDf3 = df1.select(customSelect(
      emp_data1Cols,
      requiredColumns): _*)
      .unionByName(df2.select(customSelect(
        emp_data2Cols,
        requiredColumns): _*))

    simpleschema.fields.foreach(sf => if (mergeDf3.columns.contains(sf.name.toLowerCase)) mergeDf3 = mergeDf3.withColumn(sf.name.toLowerCase(), col(sf.name.toLowerCase()).cast(sf.dataType)))

    mergeDf3
  }
  def customSelect(availableCols: Seq[String], requiredCols: Seq[String]) = {
    println("requiredCols is "+ requiredCols)
    requiredCols.toList.map(column => column match {
      case column if availableCols.contains(column) => col(column)
      case _ => lit("").as(column)
    })
  }


//  def checkifpresent(Columnlist: List , schemalist: List): Unit =
//  {
//    Columnlist.map(x=> if(schemalist.contains(x)))
//
//  }

  def getColumnDifference(df:DataFrame, schema:Array[String]): Set[String] = {
    val cols = df.columns.map{_.toLowerCase()}.toSet
    val fields = schema.map(sf => sf.toLowerCase()).toSet
    cols.diff(fields)
  }

  def getColumnDifference(schema:Array[String], df:DataFrame): Set[String] = {
    val cols = df.columns.map{_.toLowerCase()}.toSet
    val fields = schema.map(sf => sf.toLowerCase()).toSet
    fields.diff(cols)
  }
  def checkColumns5(df:DataFrame, schema:Array[String]): List[String] = {

    var listdf = Set[String]()
    val (colDiff1, colDiff2) = (getColumnDifference(df, schema), getColumnDifference(schema, df))
    if (colDiff1.nonEmpty) {
      //      val msg = "Data read from s3 has different set of columns than our schema! Differences: df-schema: %s schema-df: %s".format(
      //        colDiff1.mkString(","), colDiff2.mkString(","))
      listdf=listdf++colDiff1

      //      logger.warn(msg)
      //      BacDataProcessingResult.getInstance.addMessage(msg)
      false
    }
    if (colDiff2.nonEmpty) {
      //      val msg = "Data read from s3 has different set of columns than our schema! Differences: df-schema: %s schema-df: %s".format(
      //        colDiff1.mkString(","), colDiff2.mkString(","))
      listdf=listdf ++ colDiff2

      //      logger.warn(msg)
      //      BacDataProcessingResult.getInstance.addMessage(msg)
      //      false
    }
    listdf.toList

  }
  /**
  writing to read parquet files with different scheema
   */

  def checkNonHistoryPathparquet(objectKey:String): Boolean = {
//    val pattern = ".+\\/\\D+\\/(initial|incremental)\\/.+\\.snappy\\.parquet".r
val pattern = "parquet$".r
    pattern.findFirstIn(objectKey) match {
      case Some(_) => true
      case None => false
    }
  }
  def extractTimestampFromBlobFilePath(fileName: String): Long = {
    val pattern = "(.*)(\\d{17})(\\.dat\\.gz)$".r
    fileName match {
      case pattern(x, timeStamp, extension) => {
        timeStamp.toLong
      }
      case _ => {
        println("Unexpected S3 Object key \"{}\", did not match {}")
        0L
      }
    }
  }
  //println(checkNonHistoryPathparquet("abcd.parquet"))

//
//  def processBasedOnOpType(siteID: String, df: DataFrame, tableName: String, isInitial: Boolean): Unit = {
//    /*
//     * Processing insert and updates the same way.
//     * Ignoring deletes (optype = 3) for now. This may change in the future
//     */
//    var tabletype="custom"
//    val siteNameLog: String = "Site: "
//    println(siteNameLog + siteID + " - Processing opcode: 1 and 2 for table: " + tableName)
//    // check for the optypes
//    // displayDistinctOpTypes(df)
//
//    try {
//      var insertDf = df
//      var deleteDf: DataFrame = null
//
//      val processDelete: Boolean = true
//      println("processDelete:" + siteID + " ," + tableName + " ," + processDelete)
//
//      if (!isInitial) {
//        val primaryKey = "test_id"
//
//        val result = processUpdatesAndDeletesAndDeletesV2(tableName,insertDf,null,primaryKey,spark,siteID,true)
//        insertDf = result._1
//        deleteDf = result._2
//      }
//      println("insertdf")
//      insertDf.show()
//      println("deletedf")
//      deleteDf.show()
//
//      insertDf.persist()
//      if (!"".equalsIgnoreCase(tabletype) && "custom".equalsIgnoreCase(tabletype)) {
//        println("inserting into custompostgress")
//        //cassandraOps.insertIntoCustomPostgres(insertDf, tableName, siteID);
//      } else {
//        //cassandraOps.insertIntoCassandra(insertDf, tableName, siteID)
//        println("inserting into cassandra")
//        // insert into 3months replica schema too
//        if (!"tests".equalsIgnoreCase(tableName)) {
//          //val filteredBy3m = filterDataFrameByDate(insertDf, tableName, configs.getTimestampConfig, warmFilterMonths = "3")
//         // cassandraOps.insertIntoCassandra(filteredBy3m, tableName, siteID, serviceConfig.getReplica3mKeyspace)
//          println("For test ")
//        }
//        // TODO: Commenting this following step; This step will be activated as part of LOAD-JOB-RETRY-MODE.
//        // updateStatusOnPostgres(filteredDataFrame, true)
//
//        if (null != deleteDf && processDelete) {
//         println("We are deleting !!!!!!!!")
//        } else {
//          println("processDelete: skipping delete - " + siteID + ", " + tableName + ", " + processDelete + " - initial: " + isInitial)
//        }
//      }
//    } catch {
//
//      case e: Exception => {
//        println(siteNameLog + siteID + " - Unable to process the table: " + tableName + " with the following exception: " + e.getMessage, e)
//        // Since we have message tracking in place as part of 1.1.1 - we don't need this additional monitoring.
//        // if (metaData.isInstanceOf[StreamingMetaData]) {
//        //    updateStatusOnPostgres(df, false)
//        // }
//        throw new Exception(siteNameLog + siteID + " - Error processing at DataFrameFilterService.processDFByOpCode: " + e.getMessage, e)
//      }
//    }
//
//  }

//
//  def processUpdatesAndDeletesAndDeletesV2(tableName: String, df: DataFrame, tsColumn: String,
//                                           primaryKey: String, sparkSession: SparkSession, siteId: String,
//                                           processDelete: Boolean = true): (DataFrame, DataFrame) = {
//
//    val optypeCol = "optype"
//    var tempDf = df.withColumn("row_id", monotonically_increasing_id())
//
//    tempDf = tempDf.select(tempDf.columns.map(x => col(x).as(x.toLowerCase)): _*)
//
//    val validOptype = Seq(0,1,2,8)
//    var actualDf = tempDf.filter(trim(tempDf.col(optypeCol)).isin(validOptype:_*))
//
//    val byIdWindow: WindowSpec = Window.partitionBy(primaryKey.toLowerCase())
//
//    var reducedDf = actualDf.withColumn("max_hvr_cap_timestamp", max(hvrCapTimestampCol).over(byIdWindow))
//      .withColumn("opcode_values", collect_set(optypeCol).over(byIdWindow))
//      .filter(col("max_hvr_cap_timestamp") === col(hvrCapTimestampCol))
//      .dropDuplicates
//    reducedDf = reducedDf.withColumn("max_row_id", max("row_Id").over(byIdWindow))
//      .filter(col("max_row_id") === col("row_id"))
//      .dropDuplicates
//
//    reducedDf = reducedDf.drop("max_hvr_cap_timestamp").drop("max_row_id") // As we added these additional cols, We need to drop those here.
//    val dsResult = reducedDf.filter(trim(reducedDf.col(optypeCol)).equalTo(1).or(trim(reducedDf.col(optypeCol)).equalTo(2)))
//    val dsDeletes = reducedDf.filter(trim(reducedDf.col(optypeCol)).equalTo(0).or(trim(reducedDf.col(optypeCol)).equalTo(8)))
//
//    (dsResult, dsDeletes)
//  }

}
