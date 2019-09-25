package com.thunderhemu.PartFileCompactor

import org.apache.spark.sql.SparkSession

class Compactor(spark : SparkSession)  {

  /**
   * acts main function
   * @param ConfigFilePath path of yaml string
   */
  def process ( ConfigFilePath : String ) : Unit = {
    val util = new Util(spark)
    require(ConfigFilePath != null, "Invalid config file, config file can't be null")
    require(util.fileExists(ConfigFilePath), "Invalid config file, config file doesn't exists")
    val yamlString = spark.sparkContext.wholeTextFiles(ConfigFilePath).map(x => x._2).collect()(0)
    val constants = new Constants(yamlString)
    validation(constants,util)
    util.copyToTempLocation(constants.INPUT_FILE_PATH,constants.TEMP_FILE_PATH)
    val rawDf = util.readFileTODf(util.tempStoragePath,constants.INPUT_FORMAT)
    util.WriteDfAsFile(rawDf,constants.OUTPUT_FORMAT,constants.INPUT_FILE_PATH,"overwrite")
    util.deleteHadoopDirectory(util.tempStoragePath)
  }

  /**
   *  validates the input parameters
   * @param constants constants instance
   * @param util util instance
   */
  def validation( constants: Constants,util: Util) : Unit = {
    require(constants.INPUT_FILE_PATH != null, "Invalid input file path, input file path can't be null")
    require(util.fileExists(constants.INPUT_FILE_PATH),"Invalid input file path, input file path doesn't exists")
    require(constants.INPUT_FORMAT != null , "Invalid file format, file format can't be null")
    require(constants.OUTPUT_FORMAT != null , "Invalid out file format, out file format can't be null")
  }
}
