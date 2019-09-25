package com.thunderhemu.PartFileCompactor

import org.yaml.snakeyaml.Yaml

class Constants(input : String) {

  private val yaml = new Yaml()
  private  var obj = new java.util.LinkedHashMap[String,String ]
  obj = yaml.load(input)

  val INPUT_FILE_PATH   : String = obj.getOrDefault("input.path",null)
  val TEMP_FILE_PATH    : String = obj.getOrDefault("temp.path",null)
  val INPUT_FORMAT      : String = obj.getOrDefault("input.file.format",null)
  val OUTPUT_FORMAT     : String = obj.getOrDefault("output.file.format",null)
}
