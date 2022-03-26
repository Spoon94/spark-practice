package com.spoon.spark.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static com.spoon.spark.core.constant.CorePracticeConstant.LIST_DATA;
import static com.spoon.spark.core.constant.CorePracticeConstant.SPECIAL_WORD_SET;
import static com.spoon.spark.core.constant.CorePracticeConstant.WIKI_OF_SPARK;

/**
 * Spark数据转换算子
 * 1.map
 * 2.mapPartitions
 * 3.mapPartitionsWithIndex
 * 4.flatMap
 * 5.filter
 *
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/3/26
 */
@Slf4j
public class TransformOpJob {

    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Transform Operation").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //parallelize内存数据
        JavaRDD<String> listDataRdd = ctx.parallelize(LIST_DATA);
        listDataRdd.foreach(data -> log.warn("The Data {} in List", data));

        //map vs mapPartitions vs mapPartitionsWithIndex
        JavaRDD<String> wikiOfSparkRdd = ctx.textFile(WIKI_OF_SPARK);
        JavaRDD<String> wordRdd = wikiOfSparkRdd.flatMap(line -> Splitter.on(" ")
            .omitEmptyStrings()
            .splitToList(line).iterator());
        JavaRDD<String> wordCacheRdd = wordRdd.cache();
        //map
        List<Tuple2<Integer, String>> mapTop5CntWordList = wordCacheRdd
            .mapToPair(word -> {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                byte[] digest = md5.digest(word.getBytes(StandardCharsets.UTF_8));
                return Tuple2.apply(Hex.encodeHexString(digest), 1);

            })
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);

        //mapPartitions
        List<Tuple2<Integer, String>> mapPartitionsTop5CntWordList = wordCacheRdd
            .mapPartitionsToPair(words -> {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                List<Tuple2<String, Integer>> wordPairList = Lists.newArrayList();
                words.forEachRemaining(word -> {
                    byte[] digest = md5.digest(word.getBytes(StandardCharsets.UTF_8));
                    wordPairList.add(Tuple2.apply(Hex.encodeHexString(digest), 1));
                });
                return wordPairList.iterator();
            })
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);

        //mapPartitionsWithIndex
        List<Tuple2<Integer, String>> mapPartitionsWithIndexTop5CntWordList = wordCacheRdd
            //测试index
            //.repartition(4)
            .mapPartitionsWithIndex((index, words) -> {
                log.warn("The partition index {}", index);
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                List<String> wordPairList = Lists.newArrayList();
                words.forEachRemaining(word -> {
                    byte[] digest = md5.digest(word.getBytes(StandardCharsets.UTF_8));
                    wordPairList.add(Hex.encodeHexString(digest));
                });
                return wordPairList.iterator();
            }, true)
            .mapToPair(word -> Tuple2.apply(word, 1))
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);

        log.warn("Map Top5 Count Word {}", mapTop5CntWordList);
        log.warn("MapPartitions Top5 Count Word {}", mapPartitionsTop5CntWordList);
        log.warn("mapPartitionsWithIndex Top5 Count Word {}", mapPartitionsWithIndexTop5CntWordList);

        //filter
        List<Tuple2<Integer, String>> noSpecialWordTop5CntWordList = wordCacheRdd.filter(word -> {
            List<String> tempWordList = Splitter.on("-").splitToList(word);
            return tempWordList.stream().noneMatch(SPECIAL_WORD_SET::contains);
        })
            .mapPartitionsToPair(words -> {
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                List<Tuple2<String, Integer>> wordPairList = Lists.newArrayList();
                words.forEachRemaining(word -> {
                    byte[] digest = md5.digest(word.getBytes(StandardCharsets.UTF_8));
                    wordPairList.add(Tuple2.apply(Hex.encodeHexString(digest), 1));
                });
                return wordPairList.iterator();
            })
            .reduceByKey(Integer::sum)
            .mapToPair(Tuple2::swap)
            .sortByKey(false)
            .take(5);
        log.warn("No Special Top5 Count Word {}", noSpecialWordTop5CntWordList);
    }

}
