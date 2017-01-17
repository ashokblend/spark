package org.apache.spark.sql.examples

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import scala.io.Source

object TestSpark {
  def main(args: Array[String]) {
    val file: File = new File(args(0))
    val basePath: String = file.getParentFile.getAbsolutePath

    // get current directory:/examples
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "../../../")
      .getCanonicalPath

    // specify parameters
    val storeLocation = basePath + "/store"
    val kettleHome = new File(currentDirectory + "/../processing/carbonplugins").getCanonicalPath
    val warehouse = s"$storeLocation/warehouse"
    val metastoredb = s"$storeLocation/metastore_db"
    // clean data folder
    if (false) {
      val clean = (path: String) => FileUtils.deleteDirectory(new File(path))
      clean(storeLocation)
      clean(warehouse)
      clean(metastoredb)
    }

    

    val spark = SparkSession
    .builder()
    .master("local")
    .appName("TestSpark")
    .config("spark.sql.warehouse.dir", warehouse)
    .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=$metastoredb;create=true")
    .getOrCreate()
    val sqlFile = new File(args(0))
    var time = sqlFile.lastModified()
    var isModified = true
    val thread = new Thread {
      while(true) {
        if(isModified) {
            try {
              Source.fromFile(sqlFile).getLines().foreach { x => processCommand(x, spark) }
            } catch {
              case ex:Exception => ex.printStackTrace()
            }
          }
          val modifiedTime= sqlFile.lastModified()
          if(time == modifiedTime) {
            isModified=false
          } else {
            isModified = true
            time = modifiedTime
          }
       
      }
    }
    
  }
  def processCommand(cmd: String, spark: SparkSession): Unit = {
    // scalastyle:off
    if (!cmd.startsWith("#")) {
      println(s"executing>>>>>>$cmd")
      val result = spark.sql(cmd)
      //result.foreach(_.get)
      result.show(100, truncate=false)
      println(s"executed>>>>>>$cmd")
    }
  }
}
