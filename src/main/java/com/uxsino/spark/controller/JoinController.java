package com.uxsino.spark.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import com.uxsino.spark.common.JoinType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import scala.collection.JavaConverters;

@Api(tags = "Join API")
@RestController
@RequestMapping("join")
public class JoinController {

    @Autowired
    private JavaSparkContext javaSparkContext;

    @ApiOperation("join表操作，可以输入join类型")
    @RequestMapping(value = "/byway",method = RequestMethod.GET)
    public String join(String jointype) {
        JavaMongoRDD<Document> rdd = MongoSpark.load(javaSparkContext);
        Dataset<Row> rdd_A1 = MongoSpark.load(javaSparkContext).toDF();

        System.out.println("##############################################");

        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", "saprk_B1");
        readOverrides.put("readPreference.name", "secondaryPreferred");
        ReadConfig readConfig = ReadConfig.create(javaSparkContext).withOptions(readOverrides);

        Dataset<Row> rdd_B1 = MongoSpark.load(javaSparkContext, readConfig).toDF();
        List<String> list = new ArrayList<>();
        list.add("行政编码");
        Long start = System.currentTimeMillis();
        Dataset<Row> joined = rdd_A1.join(rdd_B1, convert(list),
            StringUtils.isEmpty(jointype) ? JoinType.Inner.getValue() : jointype);
        joined.show();
        System.out.println(System.currentTimeMillis() - start);
        return joined.toString();
    }

    public static java.util.List<String> convert(scala.collection.immutable.Seq<String> seq) {
        return scala.collection.JavaConversions.seqAsJavaList(seq);
    }

    public static scala.collection.immutable.Seq<String> convert(java.util.List<String> tmpList) {
        scala.collection.immutable.Seq<String> tmpSeq = (scala.collection.immutable.Seq<String>) JavaConverters
            .asScalaIteratorConverter(tmpList.iterator()).asScala().toSeq();
        return tmpSeq;
    }

}
