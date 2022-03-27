package com.spoon.spark.core;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static com.spoon.spark.core.constant.CorePracticeConstant.WIKI_OF_SPARK;

/**
 * Spark数据聚合
 * 1.groupByKey
 * 2.reduceByKey
 * 3.aggregateByKey
 * 4.sortByKey
 * 5.combineByKey
 *
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/3/27
 */
@Slf4j
public class AggOpJob {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Transform Operation").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //读文件
        JavaRDD<String> rawRdd = ctx.textFile(WIKI_OF_SPARK);
        JavaRDD<String> cacheWordRdd = rawRdd.flatMap(line -> Splitter.on(" ").splitToList(line)
            .iterator())
            .filter(word -> !Strings.isNullOrEmpty(word))
            .cache();

        //groupByKey
        List<Tuple2<String, Iterable<String>>> groupByKeyWordList = cacheWordRdd.mapToPair(
            word -> Tuple2.apply(word, word))
            .groupByKey()
            .take(5);
        log.warn("GroupByKey Word {}", groupByKeyWordList);

        //reduceByKey
        List<Tuple2<String, Integer>> reduceByKeyWordList = cacheWordRdd.mapToPair(
            word -> Tuple2.apply(word, RandomUtils.nextInt(0, 100)))
            .reduceByKey(Math::max)
            .take(5);
        log.warn("ReduceByKey Word {}", reduceByKeyWordList);

        //aggregateByKey
        List<Tuple2<String, Integer>> aggregateByKeyWordList = cacheWordRdd.mapToPair(word -> Tuple2.apply(word, 1))
            .aggregateByKey(0, Integer::sum, Math::max)
            .take(5);
        log.warn("AggregateByKey Word {}", aggregateByKeyWordList);

        List<Tuple2<Integer, String>> combineByKeyTop5WordList = cacheWordRdd.mapToPair(word -> Tuple2.apply(word, 1))
            .combineByKey(cnt -> cnt, Integer::sum, Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);
        log.warn("CombineByKey Top5 Words {}", combineByKeyTop5WordList);
        //CombineByKey Top5 Words [(67,the), (63,Spark), (54,a), (51,and), (50,of)]
    }

}
