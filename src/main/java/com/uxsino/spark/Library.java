package com.uxsino.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

public class Library {
    public static void main(final String[] args) throws InterruptedException {

        SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.output.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
                .config("spark.mongodb.input.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
                .config("spark.mongodb.output.database", "stats_dev").config("spark.mongodb.output.collection", "spark_A1")
                .config("spark.mongodb.input.database", "stats_dev").config("spark.mongodb.input.collection", "spark_A1")
                .getOrCreate();

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        /*Start Example: Read data from MongoDB************************/

        // Create a custom ReadConfig
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "spark_A1");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        // Load data using the custom ReadConfig
        JavaMongoRDD<Document> customRdd = MongoSpark.load(jsc, readConfig);

        /*End Example**************************************************/

        // Analyze data from MongoDB
        System.out.println(customRdd.count());
        System.out.println("###"+customRdd.first().toJson());

        jsc.close();

      }
    }
