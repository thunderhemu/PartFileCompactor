package com.thunderhemu.PartFileCompactor.tests

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import com.thunderhemu.PartFileCompactor.{Compactor, Constants, Util}

class CompactorTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {


  val conf : SparkConf = new SparkConf().setAppName("test-compactor").setMaster("local[*]")
  var spark: SparkSession  = _
  val wareHouseInputDir = new File("src/test/resources/input/generated/")
  val wareHouseOutputDir = new File("src/test/resources/output/")

  /**
   * override beforeAll method
   */
  override def beforeAll(): Unit = {
    deleteIfExists("metastore_db")
    deleteIfExists("src/test/resources/input/orc")
    deleteIfExists("src/test/resources/input/parquet")
    spark  = SparkSession.builder.config(conf).getOrCreate()
    super.beforeAll()
  }

  /**
   * override afterAll method
   */
  override def afterAll(): Unit = {
    spark.stop()
    deleteIfExists("metastore_db")
    deleteIfExists("spark-warehouse")
  }


 def deleteIfExists(path : String) : Unit = {
   val dirPath = new File(path)
   if(dirPath.exists()){
     dirPath.listFiles().foreach(f => deleteRecursively(f))
     dirPath.delete()
   }
 }

  def getNumberOfFiles(dir: String): Int = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.size
    } else {
      0
    }
  }
  /**
   *  Delete the files in the out put directory
   * @param file file to delete
   */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  /**
   * validation method with invalid input file  null case
   */
  test("validation invalid input file null case") {
    val constants = new Constants(spark.sparkContext
      .wholeTextFiles("src/test/resources/input/configs/invalid-input-path.yaml").map(x => x._2).collect()(0))
    val util = new Util(spark)
    val thrown = intercept[Exception]{
      new Compactor(spark).validation(constants,util)
    }
    assert(thrown.getMessage == "requirement failed: Invalid input file path, input file path can't be null")
  }

  /**
   * validation method with invalid input file invalid path case
   */
  test("validation invalid input file invalid path case") {
    val constants = new Constants(spark.sparkContext
      .wholeTextFiles("src/test/resources/input/configs/invalid-path.yaml").map(x => x._2).collect()(0))
    val util = new Util(spark)
    val thrown = intercept[Exception]{
      new Compactor(spark).validation(constants,util)
    }
    assert(thrown.getMessage == "requirement failed: Invalid input file path, input file path doesn't exists")
  }

  /**
   * validation method with invalid input file format
   */
  test("validation invalid input file format") {
    val constants = new Constants(spark.sparkContext
      .wholeTextFiles("src/test/resources/input/configs/invalid-input-format.yaml").map(x => x._2).collect()(0))
    val util = new Util(spark)
    val thrown = intercept[Exception]{
      new Compactor(spark).validation(constants,util)
    }
    assert(thrown.getMessage == "requirement failed: Invalid file format, file format can't be null")
  }

  /**
   * validation method with invalid output file format
   */
  test("validation invalid output file format") {
    val constants = new Constants(spark.sparkContext
      .wholeTextFiles("src/test/resources/input/configs/invalid-out-file-format.yaml").map(x => x._2).collect()(0))
    val util = new Util(spark)
    val thrown = intercept[Exception]{
      new Compactor(spark).validation(constants,util)
    }
    assert(thrown.getMessage == "requirement failed: Invalid out file format, out file format can't be null")

  }

  /**
   * testing whole process with orc
   */
  test(" orc compaction"){
    val yamlPath ="src/test/resources/input/configs/valid-orc-config.yaml"
    val inputPath  = "src/test/resources/input/data/orc/sampledata"
    val sampleData = "src/test/resources/input/data/sampledata.csv"
    val rawDf = spark.read.format("csv").option("delimiter", ",")
      .option("ignoreTrailingWhiteSpace","true").option("ignoreLeadingWhiteSpace","true")
      .option("inferSchema", "true").option("header", "true").load(sampleData)
    rawDf.show()
    rawDf.repartition(199).write.mode("overwrite").orc(inputPath)
    val pretCount = getNumberOfFiles(inputPath)
    new Compactor(spark).process(yamlPath)
    val postCount = getNumberOfFiles(inputPath)
    assert(pretCount > postCount)
    deleteIfExists("src/test/resources/input/data/orc")
  }

  /**
   * testing whole process with parquet
   */
  test(" parquet compaction"){
    val yamlPath = "src/test/resources/input/configs/valid-parquet-config.yaml"
    val inputPath  = "src/test/resources/input/data/parquet/sampledata"
    val sampleData = "src/test/resources/input/data/sampledata.csv"
    val rawDf = spark.read.format("csv").option("delimiter", ",")
      .option("ignoreTrailingWhiteSpace","true").option("ignoreLeadingWhiteSpace","true")
      .option("inferSchema", "true").option("header", "true").load(sampleData)
    rawDf.show()
    rawDf.repartition(199).write.mode("overwrite").parquet(inputPath)
    val pretCount = getNumberOfFiles(inputPath)
    new Compactor(spark).process(yamlPath)
    val postCount = getNumberOfFiles(inputPath)
    assert(pretCount > postCount)
    deleteIfExists("src/test/resources/input/data/parquet")
  }

  /**
   * yaml file null case
   */
  test("yaml file null case"){
    val thrown = intercept[Exception]{
      new Compactor(spark).process(null)
    }
    assert(thrown.getMessage=="requirement failed: Invalid config file, config file can't be null")
  }

  /**
   * invalid yaml file location case
   */
  test("invalid yaml file case"){
    val thrown = intercept[Exception]{
      new Compactor(spark).process("src/test/resources/invalid-file.yaml")
    }
    assert(thrown.getMessage=="requirement failed: Invalid config file, config file doesn't exists")
  }




}
