package com.spoon.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/4/5
 */
public class SqlJob {
    public static void main(String[] args) throws AnalysisException {
        SparkConf conf = new SparkConf().setAppName("Car Num Analyse").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        SparkSession session = new SparkSession(ctx.sc());
        //1.createDataFrame
        JavaRDD<Tuple2<String, Integer>> rdd = ctx.parallelize(
            Lists.newArrayList(Tuple2.apply("Bob", 14), Tuple2.apply("Alice", 18)));
        StructType schema = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("age", DataTypes.IntegerType, true)
        ));
        JavaRDD<Row> rowRdd = rdd.map(tuple -> RowFactory.create(tuple._1(), tuple._2()));
        Dataset<Row> dataFrame = session.createDataFrame(rowRdd, schema);

        dataFrame.createTempView("t1");
        Dataset<Row> result = session.sql("select * from t1");
        result.show();

    }
}
