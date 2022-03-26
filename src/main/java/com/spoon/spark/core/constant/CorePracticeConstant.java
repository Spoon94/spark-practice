package com.spoon.spark.core.constant;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/3/26
 */
public class CorePracticeConstant {
    public static final String WIKI_OF_SPARK = "src/main/resources/data/wikiOfSpark.txt";
    public static final List<String> LIST_DATA =
        Lists.newArrayList("Spark", "is", "cool");
    public static final Set<String> SPECIAL_WORD_SET =
        Sets.newHashSet("&", "|", "#", "^", "@");

}
