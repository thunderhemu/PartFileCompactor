package com.thunderhemu.PartFileCompactor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utils like fileExists Method --> to check if exists
 *            readFiletoDf  --> reads file and returns DataFrame
 *            WriteDfAsFile --> writes DataFrame as file
 * @param spark SparkSession
 */

class Util(spark : SparkSession) {

  var csvDelimiter : String = ","
  var BLOCK_SIZE = 134217728 // default bloc size is 128 MB
  var tempStoragePath = ""

  private val PARQUET_RATIO = 2.0 // (100 / 2.0) = 50.0 ~ 50% compression rate on text
  private val ORC_RATIO = 2.0 // (100 / 2.0) = 50.0 ~ 50% compression rate on text
  private val TEXT_RATIO = 1.0

  /**
   *  method to check if file exists or not
   * @param path file name with path
   * @return true -> file exist , false -> if file doesn't exist
   */
  def fileExists(path: String): Boolean = {
    val hConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hConf)
    fs.exists(new Path(path))
  }

  def deleteHadoopDirectory(path : String) : Boolean ={
    val hConf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hConf)
    fs.deleteOnExit(new Path(path))
  }

  /**
   * Reads file and returns data frame
   * @param inputFile input file name with full path
   * @param format input file format
   * @return  DataFrame dataFrame read out of input file
   */
  def readFileTODf(inputFile : String,format : String) : DataFrame  = format.toLowerCase match {
    case "orc" => spark.read.orc(inputFile)
    case "parquet" => spark.read.parquet(inputFile)
    case "csv"   => spark.read.format("csv").option("delimiter", csvDelimiter)
                    .option("ignoreTrailingWhiteSpace","true").option("ignoreLeadingWhiteSpace","true")
                    .option("inferSchema", "true").option("header", "true").load(inputFile)
    case "tsv"  => spark.read.option("sep", "\t").option("inferSchema", "true").option("header", "true").csv(inputFile)
    case _ => throw new IllegalArgumentException("Invalid file format, this format is not supported at the moment")
  }

  /**
   *  calculates the number of part files to be written
   * @param df input data frame
   * @return
   */
  def getPartFileCount( df : DataFrame,format : String ) : Int =
    Math.ceil(calcRDDSize(df.rdd.map(_.toString())) / (BLOCK_SIZE * getCompressionRation(format))).toInt

  /**
   *  return the compression ration basing on file format
   * @param format file format
   * @return
   */
  def getCompressionRation(format : String) : Double = format.toLowerCase match {
    case "parquet" => PARQUET_RATIO
    case "orc"     => ORC_RATIO
    case "csv"     => TEXT_RATIO
    case "tsv"     => TEXT_RATIO
    case _         => 1.0
  }

  /**
   * Writes the df to target location
   * @param df  data frame to write
   * @param format format to be written
   * @param location path with file name where it has be written
   * @param mode mode to overwrite / append
   */
  def WriteDfAsFile(df : DataFrame,format : String,location : String,mode : String) : Unit = format.toLowerCase match {
    case "csv"     => df.repartition(getPartFileCount(df,format)).write.mode(mode).option("header", "true").csv(location)
    case "tsv"     => df.repartition(getPartFileCount(df,format)).write.mode(mode).option("header", "true").option("sep", "\t").csv(location)
    case "parquet" => df.repartition(getPartFileCount(df,format)).write.mode(mode).parquet(location)
    case "orc"     => df.repartition(getPartFileCount(df,format)).write.mode(mode).orc(location)
    case "json"    => df.repartition(getPartFileCount(df,format)).write.mode(mode).json(location)
    case _         => throw new IllegalArgumentException("Invalid format, currently this output " +format +" format is not supported")
  }

  /**
   * copies data from one hdfs location to hdfs another location
   * @param srcPath source path
   * @param dstPath destination path
   */
  def copyToTempLocation(srcPath: String, dstPath: String) : Unit = {
    tempStoragePath = dstPath + "/" + srcPath.split("/").last
    require(!fileExists(tempStoragePath),"Invalid temp location, temp path " +tempStoragePath + " exists")
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val sourcPath = new Path(srcPath)
    val destPath = new Path(tempStoragePath)
    hdfs.rename(sourcPath, destPath)
  }

  /**
   * calculates the size of rdd
   * @param rdd rdd to calculate the size
   * @return size of rdd
   */
  def calcRDDSize(rdd: RDD[String]): Long = rdd.map(_.getBytes("UTF-8").length.toLong)
                                             .reduce(_+_) //add the sizes together
}

