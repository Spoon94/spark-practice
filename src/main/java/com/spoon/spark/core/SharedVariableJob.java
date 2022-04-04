package com.spoon.spark.core;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import static com.spoon.spark.core.constant.CorePracticeConstant.WIKI_OF_SPARK;

/**
 * 共享变量
 *
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/4/4
 */
@Slf4j
public class SharedVariableJob {
    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Shared Variable Operation").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //version 1:不使用广播变量
        List<String> specialWordList = Lists.newArrayList("Apache", "Spark");
        List<Tuple2<String, Integer>> specialWordCnt = ctx.textFile(WIKI_OF_SPARK)
            .flatMap(line -> Splitter.on(" ").splitToList(line).iterator())
            .filter(specialWordList::contains)
            .mapToPair(word -> Tuple2.apply(word, 1))
            .reduceByKey(Integer::sum)
            .collect();
        log.warn("The no shared variable special words cnt list {}", specialWordCnt);

        //version 2:使用广播变量
        Broadcast<List<String>> bc = ctx.broadcast(specialWordList);
        List<Tuple2<String, Integer>> specialWordCntV2 = ctx.textFile(WIKI_OF_SPARK)
            .flatMap(line -> Splitter.on(" ").splitToList(line).iterator())
            .filter(word -> bc.value().contains(word))
            .mapToPair(word -> Tuple2.apply(word, 1))
            .reduceByKey(Integer::sum)
            .collect();
        log.warn("The shared variable special words cnt list {}", specialWordCnt);

        //累加器
        LongAccumulator ac = ctx.sc().longAccumulator();
        ctx.textFile(WIKI_OF_SPARK)
            .flatMap(line -> Splitter.on(" ").splitToList(line).iterator())
            .filter(word -> {
                if ("".equals(word)) {
                    ac.add(1L);
                    return false;
                } else {
                    return true;
                }
            })
            .mapToPair(word -> Tuple2.apply(word, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);
        Long emptyStrCnt = ac.value();
        log.warn("The Empty String cnt is {}", emptyStrCnt);

    }

}
