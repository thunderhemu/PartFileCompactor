# FileCompactor
  This application compacts the part file of a given path

## Table of Contents
* [General info](#general-info)
* [Execution options](#execution-options)
* [Compaction ratio formula](#compaction-ratio-formula)
* [Set up local env for tests](#set-up-local-env-for-tests)
* [Project structure](#project-structure)
* [Future scope](#future-scope)

## General info
   This application compacts the part file of a given path, it copies data to temp location 
   and read it from temp location and over writes the input path with part file calculated based on the size 

### config file 
    
   save config file in hdfs location and provide this as input path     
        
     config file should contain the following
      input.path  ===>  input file path to compacted
      temp.path ===> temp hdfs path (it copies source file to temp)
      input.file.format ===> input file format
      output.file.format  ==> output file format
      
## Execution options
     spark-submit \
       --class com.thunderhemu.PartFileCompactorCompactorApp\
       --master local[*] \
       ${JAR_PATH}/PartFileCompactor-0.0.1-SNAPSHOT.jar \
       --conf spark.myApp.conf=<config file path>
      

## Compaction ratio formula

    Default Block Size = 134217728 (default block size is 128 MB)  => y
  
    Read Input Directory Total Size = x
  
    Compression Ratio => r
  
    Output Files: CEIL( x / (r * y) )  = # of Mappers

## Set up local env for tests

    set JAVA_HOME
  
    Download spark binaries( take Pre-built for Apache Hadoop) from https://spark.apache.org/downloads.html
 
   #### Linux/MAC
  
     Unzip the file and set <Unzip path>/spark-<spark-version>-bin-hadoop<hadoop-version>/bin as path 
   
   #### Windows
     Download winutils.exe  from https://github.com/steveloughran/winutils (choose the right version as spark-hadoop binary version)
   
     Unzip the file and copy the path <Unzip-binaries-path>/spark-<spark-version>-bin-hadoop<hadoop-version>/bin to path under environment variables
   
     Copy winutils.exe to <Unzip-binaries-path>/spark-<spark-version>-bin-hadoop<hadoop-version>/bin
   
     create <user-home-path>/hadoop/bin and copy winutils.exe in to and it as HADOOP_HOME  under environment variables
   
## Project structure 

       ├── src          # Source files
       |   ├──  main    # main class
       |   |    ├──  resources  # config templates 
       |   |    └──  scala      # Acutal code
       |   └──  tests   # Unit tests
       |        ├──  resources  # test resources required for test cases
       |        └──  scala      # Unit test cases
       ├── build.sbt     # build file with dependencies
       ├── .gitignore    # list of untracked files
       └── README.md
  
## Future scope

  At the moment we calculating default compression ration, we want to include compressionCodec as well
  
  SNAPPY_RATIO = 1.7;     // (100 / 1.7) = 58.8 ~ 40% compression rate on text
  
  LZO_RATIO = 2.0;        // (100 / 2.0) = 50.0 ~ 50% compression rate on text
  
  GZIP_RATIO = 2.5;       // (100 / 2.5) = 40.0 ~ 60% compression rate on text
  
  BZ2_RATIO = 3.33;       // (100 / 3.3) = 30.3 ~ 70% compression rate on text
  
  AVRO_RATIO = 1.6;       // (100 / 1.6) = 62.5 ~ 40% compression rate on text
  
  PARQUET_RATIO = 2.0;    // (100 / 2.0) = 50.0 ~ 50% compression rate on text
 
   ### Compression Ratio Formula
  
  Input Compression Ratio * Input Serialization Ratio * Input File Size = Input File Size Inflated
  
  Input File Size Inflated / ( Output Compression Ratio * Output Serialization Ratio ) = Output File Size
  
  Output File Size / Block Size of Output Directory = Number of Blocks Filled
  
  FLOOR( Number of Blocks Filled ) + 1 = Efficient Number of Files to Store