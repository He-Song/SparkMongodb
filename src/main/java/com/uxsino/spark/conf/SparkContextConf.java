package com.uxsino.spark.conf;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkContextConf {

    @Bean(name = "javaSparkContext")
    public JavaSparkContext getSparkContext() {
        SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
            .config("spark.mongodb.output.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.input.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.output.database", "stats_dev").config("spark.mongodb.output.collection", "spark_A1")
            .config("spark.mongodb.input.database", "stats_dev").config("spark.mongodb.input.collection", "spark_A1")
            .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        return jsc;
    }

}
