package com.matthew.localSpark

import java.io.File
import java.util.Map.Entry

import com.typesafe.config.{ConfigFactory, ConfigObject, ConfigValue}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.{ArrayBuffer, _}
import scala.math.BigDecimal.RoundingMode
import scala.collection.JavaConverters._
import scala.util.control.Breaks._


class budgetAnalyser(csvString1: String,
                     csvString2: String = "",
                     properties: String,
                     sc: SQLContext,
                     display: Boolean = false,
                     writeToDisk: Boolean = false,
                     writeToPath: String = ""
                     ) {
  val catMap = new LinkedHashMap[String, ArrayBuffer[budgetRow]]
  val income = new ArrayBuffer[budgetRow]
  val outgoings = new ArrayBuffer[budgetRow]
  val catTotals = new LinkedHashMap[String, Double]
  val result = new ArrayBuffer[budgetRow]
  val totalsAndTransactions = new ListBuffer[(String, Double, String)]

  {
    val loadedCSV1 = loadCSV(csvString1)
    var loadedCSV2 = sc.emptyDataFrame
    if(!csvString2.isEmpty){
      loadedCSV2 = loadedCSV1.union(loadCSV(csvString2))
    }

    if(loadedCSV2.collectAsList.size>loadedCSV1.collectAsList.size()){
      setSpendingCategories(loadedCSV2)
    }
    else{
      setSpendingCategories(loadedCSV1)
    }

    for (entries <- mapCategories(catMap)) catTotals += entries

    val myList = new ListBuffer[ArrayBuffer[budgetRow]]
    for ((k,v)<- catMap) myList +=v
    for (elem <- myList.toList.reduce(_.union(_))) result += elem

    buildFinalTable(catMap)

    result
  }

  private def setSpendingCategories(df: DataFrame) = {
    val parsedConfig = ConfigFactory.parseFile(new File(properties))
    val conf = ConfigFactory.load(parsedConfig).getObjectList("categorised").asScala
    val confMap = (for {
      item: ConfigObject <- conf
      entry: Entry[String, ConfigValue] <- item.entrySet().asScala
      key = entry.getKey
      regex = entry.getValue.unwrapped.toString

    } yield (key, regex)).toList
    import sc.implicits._
    df.as[budgetRow].rdd.collect().foreach(br => breakable {
      for ((k, v) <- confMap)
        br match {
          case _ if br.description.matches(v) && !br.value.contains("+") => catMap.getOrElseUpdate(k, new ArrayBuffer[budgetRow])+=br; break
          case _ if br.Type.matches(v) && !br.value.contains("+") => catMap.getOrElseUpdate(k, new ArrayBuffer[budgetRow])+=br; break
          case _ if br.Type.contains("CREDIT IN") => income+=br; break
          case _ =>
        }
    })

  }

  private def buildFinalTable(catMap: LinkedHashMap[String, ArrayBuffer[budgetRow]]) ={
    for (elem <- catMap) {totalsAndTransactions+=((elem._1,catTotals.getOrElse(elem._1, 0.0), elem._2.mkString(" ").replaceAll("budgetRow", "")))}
    totalsAndTransactions+=(("Total Outgoings", BigDecimal(catTotals.getOrElse("Total Calculated", 0.0)).setScale(2, RoundingMode.HALF_UP).toDouble, ""))
    totalsAndTransactions+=(("Income", categoryTotal(income), ""))
    totalsAndTransactions+=(("Net Income", BigDecimal(categoryTotal(income)-catTotals.getOrElse("Total Calculated", 0.0)).setScale(2, RoundingMode.HALF_UP).toDouble, ""))
  }

  private def mapCategories(catMap: LinkedHashMap[String, ArrayBuffer[budgetRow]]): LinkedHashMap[String, Double] = {
    val totals = new LinkedHashMap[String, Double]
    for ((key, value) <- catMap) {
      totals.put(key, categoryTotal(value))
    }
    val outGoing: Double = totals.foldLeft(0.0)(_ + _._2)
    totals.put("Total Calculated", outGoing)
    totals
  }

  private def categoryTotal(array: ArrayBuffer[budgetRow]): Double = {

    var sum = 0.0
    for (row <- array) {
      sum += row.value.toString.replaceAll("[^0-9.]", "").toDouble
    }
    BigDecimal(sum).setScale(2, RoundingMode.HALF_UP).toDouble
  }

  def loadCSV(csvLocation: String): DataFrame = {
    val df = sc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("mode", "DROPMALFORMED")
      .load(csvLocation)
    df.show(df.count().toInt, false)
    df
  }


}
