package com.thunderhemu.PartFileCompactor.tests

import java.io.File

import com.thunderhemu.PartFileCompactor.Util
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}

class UtilTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {

  val conf : SparkConf = new SparkConf().setAppName("test-Util").setMaster("local[*]")
  var spark: SparkSession  = _
  val wareHouseInputDir = new File("src/test/resources/input/generated/")
  val wareHouseOutputDir = new File("src/test/resources/output/")

  /**
   * override beforeAll method
   */
  override def beforeAll(): Unit = {
    deleteIfExists("metastore_db")
    spark  = SparkSession.builder.config(conf).getOrCreate()
    super.beforeAll()
  }

  /**
   * override afterAll method
   */
  override def afterAll(): Unit = {
    spark.stop
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
   * fileExist --> Valid case
   */
  test("Utils file exist method valid file case ") {
    assert(new Util(spark).fileExists("src/test/resources/input/file.txt"))
  }

  /**
   * fileExists --> Invalid case
   */
  test("Utils file exist method invalid file case ") {
    assert(!new Util(spark).fileExists("src/test/resources/input/invalidfile.txt"))
  }

  /**
   * readFileTODf --> csv case
   */
  test("readFileTODf csv file") {
    assert(
      new Util(spark).readFileTODf("src/test/resources/input/data/sampledata.csv","csv").count() > 0
    )
  }

  /**
   * readFileTODf --> tsv case
   */
  test("readFileTODf tsv file") {
    val rawDf = spark.read.format("csv")
            .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.option("sep", "\t")
           .option("header", "true").csv("src/test/resources/input/generated/sampledata.csv")
    assert(
      new Util(spark).readFileTODf("src/test/resources/input/generated/sampledata.csv","csv").count() > 0
    )
    wareHouseInputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * readFileTODf --> parquet case
   */
  test("readFileTODf parquet file") {
    val rawDf = spark.read.format("csv")
                 .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.parquet("src/test/resources/input/generated/sampledata-parquet")
    assert(
      new Util(spark)
        .readFileTODf("src/test/resources/input/generated/sampledata-parquet","parquet").count() > 0
    )
    wareHouseInputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * readFileTODf --> orc case
   */
  test("readFileTODf orc file") {
    val rawDf = spark.read.format("csv")
             .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.orc("src/test/resources/input/generated/sampledata-orc")
    assert(
      new Util(spark).readFileTODf("src/test/resources/input/generated/sampledata-orc","orc").count() > 0
    )
    wareHouseInputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * readFileTODf invalid file format
   */
  test("Invalid file format"){
    val thrown = intercept[Exception]{
      new Util(spark).readFileTODf("src/test/resources/input/generated/sampledata-orc","json")
    }
    assert(thrown.getMessage == "Invalid file format, this format is not supported at the moment")
  }

  /**
   * WriteDfAsFile invalid file format
   */
  test("Invalid output file format"){
    val thrown = intercept[Exception]{
      new Util(spark).WriteDfAsFile(null.asInstanceOf[DataFrame],"Avro","","")
    }
    assert(thrown.getMessage == "Invalid format, currently this output Avro format is not supported")
  }

  /**
   * WriteDfAsFile parquet file format
   */
  test("valid output file format parquet "){
    val outputFile = "src/test/resources/output/outfile-parquet"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    new Util(spark).WriteDfAsFile(rawDf,"parquet",outputFile,"overwrite")
    val resultDf = spark.read.parquet(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile parquet file format overwrite mode
   */
  test("valid output file format parquet overwrite"){
    val outputFile = "src/test/resources/output/outfile-parquet"
    val rawDf = spark.read.format("csv")
                 .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.parquet(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"parquet",outputFile,"overwrite")
    val resultDf = spark.read.parquet(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile parquet file format append mode
   */
  test("valid output file format parquet append"){
    val outputFile = "src/test/resources/output/outfile-parquet"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.parquet(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"parquet",outputFile,"append")
    val resultDf = spark.read.parquet(outputFile)
    assert(rawDf.count == resultDf.count() / 2 )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile orc file format
   */
  test("valid output file format orc "){
    val outputFile = "src/test/resources/output/outfile-orc"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    new Util(spark).WriteDfAsFile(rawDf,"orc",outputFile,"overwrite")
    val resultDf = spark.read.orc(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile orc file format overwrite mode
   */
  test("valid output file format orc overwrite"){
    val outputFile = "src/test/resources/output/outfile-orc"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.orc(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"orc",outputFile,"overwrite")
    val resultDf = spark.read.orc(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile orc file format append mode
   */
  test("valid output file format orc append"){
    val outputFile = "src/test/resources/output/outfile-orc"
    val rawDf = spark.read.format("csv")
                 .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.orc(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"orc",outputFile,"append")
    val resultDf = spark.read.orc(outputFile)
    assert(rawDf.count == resultDf.count() / 2 )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile csv file format
   */
  test("valid output file format csv "){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    new Util(spark).WriteDfAsFile(rawDf,"csv",outputFile,"overwrite")
    val resultDf = spark.read.format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile csv file format overwrite mode
   */
  test("valid output file format csv overwrite"){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.option("header", "true").csv(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"csv",outputFile,"overwrite")
    val resultDf = spark.read.format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile csv file format append mode
   */
  test("valid output file format csv append"){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.coalesce(1).write.option("header", "true").csv(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"csv",outputFile,"append")
    val resultDf = spark.read.format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count() / 2 )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile tsv file format
   */
  test("valid output file format tsv "){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
                .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    new Util(spark).WriteDfAsFile(rawDf,"tsv",outputFile,"overwrite")
    val resultDf = spark.read.option("sep", "\t").option("inferSchema", "true")
      .format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count()  )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile tsv file format overwrite mode
   */
  test("valid output file format tsv overwrite"){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
               .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.option("sep", "\t").option("header", "true").csv(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"tsv",outputFile,"overwrite")
    val resultDf = spark.read.format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile tsv file format append mode
   */
  test("valid output file format tsv append"){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv").option("inferSchema", "true")
      .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.coalesce(1).write.option("sep", "\t").option("header", "true").csv(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"tsv",outputFile,"append")
    val resultDf = spark.read.option("inferSchema", "true")
      .option("sep", "\t").format("csv").option("header", "true").load(outputFile)
    assert(rawDf.count == resultDf.count / 2 )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile json file format
   */
  test("valid output file format json "){
    val outputFile = "src/test/resources/output/outfile.csv"
    val rawDf = spark.read.format("csv")
                 .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    new Util(spark).WriteDfAsFile(rawDf,"json",outputFile,"overwrite")
    val resultDf = spark.read.json(outputFile)
    assert(rawDf.count == resultDf.count()  )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile json file format overwrite mode
   */
  test("valid output file format json overwrite"){
    val outputFile = "src/test/resources/output/outfile.json"
    val rawDf = spark.read.format("csv")
                 .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.json(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"json",outputFile,"overwrite")
    val resultDf = spark.read.json(outputFile)
    assert(rawDf.count == resultDf.count() )
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * WriteDfAsFile json file format append
   */
  test("valid output file format json append "){
    val outputFile = "src/test/resources/output/outfile.json"
    val rawDf = spark.read.format("csv").option("inferSchema", "true")
      .option("header", "true").load("src/test/resources/input/data/sampledata.csv")
    rawDf.write.json(outputFile)
    new Util(spark).WriteDfAsFile(rawDf,"json",outputFile,"append")
    val resultDf = spark.read.json(outputFile)
    assert(rawDf.count == resultDf.count / 2)
    wareHouseOutputDir.listFiles().foreach(f => deleteRecursively(f))
  }

  /**
   * testing copyToTempLocation method
   */
  test("test copyToTempLocation "){
    val srcPath = "src/test/resources/input/data/sampledata.csv"
    val destPath = "src/test/resources/output"
    val util = new Util(spark)
    util.copyToTempLocation(srcPath,destPath)
    assert(util.fileExists(destPath + "/sampledata.csv"))
    util.copyToTempLocation(destPath + "/sampledata.csv","src/test/resources/input/data/")
  }

  /**
   * testing copyToTempLocation method temp path exists
   */
  test("test copyToTempLocation temp path exists case "){
    val srcPath = "src/test/resources/input/data/sampledata.csv"
    val destPath = "src/test/resources/input"
    val util = new Util(spark)
    val thrown =  intercept[Exception] {
      util.copyToTempLocation(srcPath,destPath)
    }
    assert(thrown.getMessage=="requirement failed: Invalid temp location, temp path " +destPath+ "/sampledata.csv" +" exists")
  }

  test("deleteHadoopDirectory method") {
    val srcPath = "src/test/resources/input/data/sampledata.csv"
    val destPath = "src/test/resources/output"
    val rawDf = spark.read.format("csv").option("delimiter", ",")
      .option("ignoreTrailingWhiteSpace","true").option("ignoreLeadingWhiteSpace","true")
      .option("inferSchema", "true").option("header", "true").load(srcPath)
    rawDf.show()
    rawDf.write.mode("overwrite").parquet(destPath)
    val util = new Util(spark)
    assert(util.deleteHadoopDirectory(destPath))
  }

}
