package com.matthew.localSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

/**
  * Created by Matt on 01/08/2017.
  */
object budgetAnalyserRunner{
 def main(args:Array[String]): Unit = {
   val conf = new SparkConf().setMaster("local").setAppName("csvLoader")
   val spark = new SparkContext(conf)
   val sc = new SQLContext(spark)
   val opts = budgetAnalyserOpts
   val parser = new CmdLineParser(opts)
   try {
     parser.parseArgument(args.toList)
     val outputFile = opts.writePath
     val filename = opts.writePath.split("/")(opts.writePath.split("/").length - 1)+".csv"
     val outputFileName = outputFile + "/temp_" + filename
     val mergedFileName = outputFile + "/merged_" + filename
     val mergeFindGlob = outputFileName
     val ba = new budgetAnalyser(opts.csvPath1, opts.csvPath2, opts.propertiesFile, sc, opts.display, opts.writeToDisk, opts.writePath)
     val df1 = sc.createDataFrame(ba.totalsAndTransactions)
     val categoryNames = Seq("Category", "Total", "Transaction")
     val flattened = df1.toDF(categoryNames: _*)
     flattened.show(flattened.count.toInt, false)

     if (opts.writeToDisk) {
       flattened.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").mode("overwrite").save(outputFileName)
       merge(mergeFindGlob, mergedFileName)
       flattened.unpersist()
     }
   }
   catch {
     case e: CmdLineException =>
       print(s"Error:${e.getMessage}\n Usage \n")
       parser.printUsage(System.out)
       System.exit(1)
   }


   def merge(srcPath: String, dstPath: String): Unit = {
     val hadoopConfig = new Configuration()
     val hdfs = FileSystem.get(hadoopConfig)
     FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
     // the "true" setting deletes the source files once they are merged into the new output
   }
 }

}

