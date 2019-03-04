package com.uxsino.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import scala.Tuple2;
import scala.collection.JavaConverters;

public class SparkMongoTest implements Serializable {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnectorIntro")
            .config("spark.mongodb.output.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.input.uri", "mongodb://stats:stats2018@127.0.0.1:27017/admin")
            .config("spark.mongodb.output.database", "stats_dev").config("spark.mongodb.output.collection", "spark_A1")
            .config("spark.mongodb.input.database", "stats_dev").config("spark.mongodb.input.collection", "spark_A1")
            .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        new SparkMongoTest().test1(jsc);

        /*jsc.setLogLevel("WARN");
        Dataset<Row> rdd_A1 = MongoSpark.load(jsc).toDF();
        
        // SparkConf.SparkContext.setLogLevel("WARN");
        
        // Create a JavaSparkContext using the SparkSession's SparkContext object
         // Create a custom WriteConfig
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
        MongoSpark.save(sparkDocuments, writeConfig);
        End Example*************************************************
        System.out.println("##############################################");
        
        Start Example: Read data from MongoDB***********************
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
        System.out.println("asd===" + rdd.count());
        System.out.println("asd===" + rdd.first().toJson());
        
        jsc.close();*/

    }

    public static java.util.List<String> convert(scala.collection.immutable.Seq<String> seq) {
        return scala.collection.JavaConversions.seqAsJavaList(seq);
    }

    public static scala.collection.immutable.Seq<String> convert(java.util.List<String> tmpList) {
        scala.collection.immutable.Seq<String> tmpSeq = (scala.collection.immutable.Seq<String>) JavaConverters
            .asScalaIteratorConverter(tmpList.iterator()).asScala().toSeq();
        return tmpSeq;
    }

    public void test1(JavaSparkContext javaSparkContext) {
        // rdd1
        JavaPairRDD<String, String> rdd1 = MongoSpark.load(javaSparkContext)
            .mapToPair(new PairFunction<Document, String, String>() {

                @Override
                public Tuple2<String, String> call(Document document) throws Exception {
                    String sp = (String) document.get("地区");
                    String people = (String) document.get("人口");
                    String driverId = (String) document.get("行政编码");
                    return new Tuple2<String, String>(driverId, sp + "," + people);
                }
            });

        // rdd2
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "saprk_B1");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(javaSparkContext).withOptions(readOverrides);

        JavaPairRDD<String, String> rdd2 = MongoSpark.load(javaSparkContext, readConfig)
            .mapToPair(new PairFunction<Document, String, String>() {

                @Override
                public Tuple2<String, String> call(Document document) throws Exception {
                    String orderId = (String) document.get("特色");
                    String driverId = (String) document.get("行政编码");
                    return new Tuple2<String, String>(driverId, orderId);
                }
            });

        // join
        JavaPairRDD<String, Tuple2<String, String>> joinRdd = rdd1.join(rdd2);
        Iterator<Tuple2<String, Tuple2<String, String>>> it1 = joinRdd.collect().iterator();
        while (it1.hasNext()) {
            Tuple2<String, Tuple2<String, String>> item = it1.next();
            System.out.println("driver_id:" + item._1 + ", order_id:" + item._2._1 + ", car_id:" + item._2._2);
        }

        /*
         *   leftOuterJoin
         * */
        System.out.println(" ****************** leftOuterJoin *******************");
        JavaPairRDD<String, Tuple2<String, Optional<String>>> leftOuterJoinRdd = rdd1.leftOuterJoin(rdd2);
        Iterator<Tuple2<String, Tuple2<String, Optional<String>>>> it2 = leftOuterJoinRdd.collect().iterator();
        while (it2.hasNext()) {
            Tuple2<String, Tuple2<String, Optional<String>>> item = it2.next();
            System.out.println("driver_id:" + item._1 + ", order_id:" + item._2._1 + ", car_id:" + item._2._2);
        }

        /*
        *   rightOuterJoin
        * */
        System.out.println(" ****************** rightOuterJoin *******************");
        JavaPairRDD<String, Tuple2<Optional<String>, String>> rightOuterJoinRdd = rdd1.rightOuterJoin(rdd2);
        Iterator<Tuple2<String, Tuple2<Optional<String>, String>>> it3 = rightOuterJoinRdd.collect().iterator();
        while (it3.hasNext()) {
            Tuple2<String, Tuple2<Optional<String>, String>> item = it3.next();
            System.out.println("driver_id:" + item._1 + ", order_id:" + item._2._1 + ", car_id:" + item._2._2);
        }
    }
}
