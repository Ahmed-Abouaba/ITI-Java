package com.petrol.wzzaf;

import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WazzafDAO {






    public static Dataset<Row> read_csv(String path){
    // Create Spark Session to create connection to Spark
    final SparkSession spark = SparkSession.builder().appName("Jobs in Egypt Analysis").master("local[2]")
            .getOrCreate();
    //
//		// Get DataFrameReader using SparkSession
    final DataFrameReader jobsdf = spark.read();
		jobsdf.option("header", "true");

   return jobsdf.csv(path);



}}
