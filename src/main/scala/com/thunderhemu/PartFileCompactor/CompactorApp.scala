package com.thunderhemu.PartFileCompactor

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CompactorApp extends App {
 lazy val spark = SparkSession
             .builder
             .config( new SparkConf().setAppName("QDH:part-file-compactor").setMaster("local"))
             .enableHiveSupport()
             .getOrCreate()
  new Compactor(spark).process(spark.sparkContext.getConf.get("spark.myApp.conf",null))
  spark.stop()
}
