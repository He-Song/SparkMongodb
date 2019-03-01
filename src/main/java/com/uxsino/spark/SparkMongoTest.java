package com.uxsino.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.uxsino.spark.common.JoinType;

import scala.collection.JavaConverters;

public class SparkMongoTest {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
            .config("spark.mongodb.output.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.input.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.output.database", "stats_dev").config("spark.mongodb.output.collection", "spark_A1")
            .config("spark.mongodb.input.database", "stats_dev").config("spark.mongodb.input.collection", "spark_A1")
            .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        Dataset<Row> rdd_A1 = MongoSpark.load(jsc).toDF();

        // SparkConf.SparkContext.setLogLevel("WARN");

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        /* // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "spark");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
        
        // Create a RDD of 10 documents
        JavaRDD<Document> sparkDocuments = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
                (new Function<Integer, Document>() {
          public Document call(final Integer i) throws Exception {
              return Document.parse("{test: " + i + "}");
          }
        });
        
        Start Example: Save data from RDD to MongoDB****************
        MongoSpark.save(sparkDocuments, writeConfig);*/
        /*End Example**************************************************/
        System.out.println("##############################################");

        /*Start Example: Read data from MongoDB************************/
        // JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "saprk_B1");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        Dataset<Row> rdd_B1 = MongoSpark.load(jsc, readConfig).toDF();
//        rdd_A1.show();
//        rdd_B1.show();
        // Dataset<Row> joined = rdd_A1.join(rdd_B1, "行政编码");
//        Column col = new Column();
        List<String> list = new ArrayList<>();
        list.add("行政编码");
        Long start = System.currentTimeMillis();
        Dataset<Row> joined = rdd_A1.join(rdd_B1, convert(list), JoinType.LeftSemi.getValue());
        System.out.println("00000000000000000000000000000000");
        System.out.println(joined.toString());
        joined.show();
        System.out.println("11111111111111111111111111111111");
        System.out.println(System.currentTimeMillis()-start);
        // JavaPairRDD<String, Tuple2<String, String>> joinRdd = rdd_A1.
        // End Example*************************************************

        // Analyze data from MongoDB
        /*System.out.println("asd===" + rdd.count());
        System.out.println("asd===" + rdd.first().toJson());*/

        jsc.close();

    }

    public static java.util.List<String> convert(scala.collection.immutable.Seq<String> seq) {
        return scala.collection.JavaConversions.seqAsJavaList(seq);
    }
    
    public static scala.collection.immutable.Seq<String> convert(java.util.List<String> tmpList){
        scala.collection.immutable.Seq<String> tmpSeq = (scala.collection.immutable.Seq<String>) JavaConverters.asScalaIteratorConverter(tmpList.iterator()).asScala().toSeq();
        return tmpSeq;
    }

}
