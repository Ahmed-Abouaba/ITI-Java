//package com.petrol.wzzaf;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.DataFrameReader;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.time.Duration;
//import java.time.LocalTime;
//import java.util.*;
//import java.util.stream.Collectors;
//
//public class RddTest {
//
//    public static void main(String[] args) {
//
//
//    // Create Spark Session to create connection to Spark
//    final SparkSession spark = SparkSession.builder().appName("Jobs in Egypt Analysis").master("local[1]")
//            .getOrCreate();
//    //
////		// Get DataFrameReader using SparkSession
//    final DataFrameReader df = spark.read();
//    // Set header option to true to specify that first row in file contains
//    // name of columns
//		df.option("header", "true");
////		//Dataset<Row> airbnbDF = df.csv ("src/main/resources/Wuzzuf_Jobs.csv");
//
//    //
////		// Print Schema to see column names, types and other metadata
////		// airbnbDF.printSchema ();
//    Dataset<Row> d = df.csv("src/main/resources/Wuzzuf_Jobs.csv");
//    JavaRDD<String> r =d.select("Skills").toJavaRDD().map(new Function<Row, String>() {
//
//        @Override
//        public String call(Row row) throws Exception {
//            return row.toString();
//        }});
//
//        JavaRDD<String> words = r.flatMap (title -> Arrays.asList (title
//                .toLowerCase ()
//                .trim ().replace("]","").replace("[","")
//                .split (", ")).iterator ());
//        System.out.println(words.toString ());
//        // COUNTING
//        Map<String, Long> wordCounts = words.countByValue ();
//        List<Map.Entry> sorted = wordCounts.entrySet ().stream ()
//                .sorted (Map.Entry.comparingByValue ()).collect (Collectors.toList ());
//
//        // DISPLAY
//        for (Map.Entry<String, Long> entry : sorted) {
//            System.out.println (entry.getKey () + " : " + entry.getValue ());
//        }
//
//    }
//
//}
//
//
//
//
//
//
//
