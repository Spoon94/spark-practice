package com.spoon.spark.core;

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Word Count
 *
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/3/26
 */
@Slf4j
public class WordCountJob {
    private static final String WIKI_OF_SPARK = "src/main/resources/data/wikiOfSpark.txt";

    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //读文件
        JavaRDD<String> rawRdd = ctx.textFile(WIKI_OF_SPARK);

        //top5 频率单词
        List<Tuple2<Integer, String>> top5CntWordList = rawRdd.flatMap(line -> Splitter.on(" ").splitToList(line)
            .iterator())
            .filter(word -> !Strings.isNullOrEmpty(word))
            .mapToPair(word -> Tuple2.apply(word, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);

        //统计行数
        log.info("Top5 Word Count {}", top5CntWordList);
    }

}