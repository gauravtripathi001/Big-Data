package com.dataflair.retail

import scala.math.random
import java.lang.Float

import org.apache.spark.sql.SparkSession

object ProductSales {
	def main(args: Array[String]) {
	  
		if (args.length < 2) {
      System.err.println("Usage: ProductSales <Input-File> <Output-File>");
      System.exit(1);
    }
		
		val spark = SparkSession
				.builder
				.appName("ProductSales")
				.getOrCreate()

    val data = spark.read.textFile(args(0)).rdd
    
    val result = data.map { line => {
      val tokens=line.split("\\t")
      
      (tokens(3),Float.parseFloat(tokens(4)))
    } }
    .reduceByKey(_+_)
    
    
    result.saveAsTextFile(args(1))
    
    spark.stop
	}
}
//bin/spark-submit --class com.dataflair.retail.ProductSales ../retailJob.jar ../Retail_Sample_Data_Set.txt ret-out-001
