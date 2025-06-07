package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class Query2 {
    public static void main(String[] args) {
        // Creating a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Query 2")
                // The standard configuration is set to yarn
                .config("spark.master", "yarn") 
                .getOrCreate();

        // Reading all Italy data (2021-2024), excluding Sweden
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true") // automatically detect data types

                // ***THIS LINE HAS TO BE EDITED ON DIFFERENT SYSTEMS!***
                .csv("hdfs://master:54310/user/hadoop/dataset/IT_202*.csv");

        // Preparing the Year-Month entry by...
        // ...converting Datetime (UTC) to timestamp in datetime column and...
        df = df.withColumn("datetime", to_timestamp(df.col("Datetime (UTC)"), "yyyy-MM-dd HH:mm:ss"));

        // ...extracting year and month from this timestamp
        df = df.withColumn("year", year(df.col("datetime")))
                .withColumn("month", month(df.col("datetime")));

        // Computing monthly averages for carbon intensity and carbon-free energy (CFE) share
        Dataset<Row> monthlyAvg = df.groupBy("year", "month")
                .agg(
                        avg("Carbon intensity gCO₂eq/kWh (direct)").alias("avg_carbon_intensity"),
                        avg("Carbon-free energy percentage (CFE%)").alias("avg_CFE")
                )
                // Ordering rows by the Year-Month-Tuples
                .orderBy("year", "month");

        // Ranking top 5 highest & lowest months for carbon intensity and CFE
        Dataset<Row> highestCarbon = monthlyAvg.orderBy(desc("avg_carbon_intensity")).limit(5);
        Dataset<Row> lowestCarbon = monthlyAvg.orderBy("avg_carbon_intensity").limit(5);
        Dataset<Row> highestCFE = monthlyAvg.orderBy(desc("avg_CFE")).limit(5);
        Dataset<Row> lowestCFE = monthlyAvg.orderBy("avg_CFE").limit(5);

        
        // Saving results to CSV files in HDFS
        // ***THESE LINES HAVE TO BE EDITED ON DIFFERENT SYSTEMS!***
        monthlyAvg.write().mode("overwrite").option("header", "true").csv("hdfs://master:54310/user/hadoop/query2_results.csv");
        highestCarbon.write().mode("overwrite").option("header", "true").csv("hdfs://master:54310/user/hadoop/query2_highest_carbon.csv");
        lowestCarbon.write().mode("overwrite").option("header", "true").csv("hdfs://master:54310/user/hadoop/query2_lowest_carbon.csv");
        highestCFE.write().mode("overwrite").option("header", "true").csv("hdfs://master:54310/user/hadoop/query2_highest_CFE.csv");
        lowestCFE.write().mode("overwrite").option("header", "true").csv("hdfs://master:54310/user/hadoop/query2_lowest_CFE.csv");

        // Stop Spark session
        spark.stop();
    }
}
