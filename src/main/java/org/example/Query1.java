package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Query1 {
    public static void main(String[] args) {
        // Creating a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Query 1")
                // The standard configuration is set to yarn
                .config("spark.master", "yarn")
                .getOrCreate();

        // Reading CSV from HDFS into a dataframe df
        Dataset<Row> df = spark.read()
        // The header won't be read
                .option("header", "true")
                .option("inferSchema", "true") // automatically detect data types
                
                // ***THIS LINE HAS TO BE EDITED ON DIFFERENT SYSTEMS!***
                .csv("hdfs://master:54310/user/hadoop/dataset/*.csv");

        // Preparing the table by adding a Year column...
        df = df.withColumn("Year", year(df.col("Datetime (UTC)")))
                //...and keeping only the relevant columns
                .select("Year","Country",
                        "Carbon intensity gCO₂eq/kWh (direct)",
                        "Carbon-free energy percentage (CFE%)");

        // Grouping by Year and Country and computing aggregate functions
        Dataset<Row> aggregatedDF = df.groupBy("Year", "Country")
                .agg(   avg("Carbon intensity gCO₂eq/kWh (direct)").alias("Avg_Carbon_Intensity"),
                        min("Carbon intensity gCO₂eq/kWh (direct)").alias("Min_Carbon_Intensity"),
                        max("Carbon intensity gCO₂eq/kWh (direct)").alias("Max_Carbon_Intensity"),
                        avg("Carbon-free energy percentage (CFE%)").alias("Avg_CFE"),
                        min("Carbon-free energy percentage (CFE%)").alias("Min_CFE"),
                        max("Carbon-free energy percentage (CFE%)").alias("Max_CFE")
                )
                // Ordering rows by the Country-Year-Tuples
                .orderBy("Country","Year");

        // Writing all results to CSV
        aggregatedDF.write()
                // Results will be written with a header again
                .option("header", "true")

                // ***THIS LINE HAS TO BE EDITED ON DIFFERENT SYSTEMS!***
                .csv("hdfs://master:54310/user/hadoop/query1_results.csv");

        // Stop Spark session
        spark.stop();
    }
}
