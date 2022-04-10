package com.spoon.spark.sql;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;

import static org.apache.spark.sql.functions.asc;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.sum;

/**
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/4/10
 */
public class DataFrameOperatorJob {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Car Num Analyse").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        SparkSession session = new SparkSession(ctx.sc());
        //1.createDataFrame
        JavaRDD<Tuple4<Integer, String, Integer, String>> rdd1 = ctx.parallelize(
            Lists.newArrayList(
                Tuple4.apply(1, "John", 26, "Male"),
                Tuple4.apply(2, "Lily", 28, "Female"),
                Tuple4.apply(3, "Raymond", 30, "Male"),
                Tuple4.apply(1, "John", 26, "Male"),
                Tuple4.apply(4, "Simon", 27, null)
            ));
        StructType schema1 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("age", DataTypes.IntegerType, true),
            DataTypes.createStructField("gender", DataTypes.StringType, true)
        ));

        JavaRDD<Tuple2<Integer, Integer>> rdd2 = ctx.parallelize(
            Lists.newArrayList(
                Tuple2.apply(1, 26000),
                Tuple2.apply(2, 30000),
                Tuple2.apply(4, 25000),
                Tuple2.apply(3, 20000)
            ));
        StructType schema2 = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("id", DataTypes.IntegerType, true),
            DataTypes.createStructField("salary", DataTypes.IntegerType, true)
        ));

        JavaRDD<Row> employeeRdd = rdd1.map(tuple ->
            RowFactory.create(tuple._1(), tuple._2(), tuple._3(), tuple._4())
        );
        Dataset<Row> employeeDF = session.createDataFrame(employeeRdd, schema1);

        JavaRDD<Row> salaryRdd = rdd2.map(tuple ->
            RowFactory.create(tuple._1(), tuple._2())
        );
        Dataset<Row> salaryDF = session.createDataFrame(salaryRdd, schema2);

        //1.探索类算子
        employeeDF.printSchema();
        employeeDF.show();
        Dataset<Row> ageDF = employeeDF.describe("age");
        ageDF.show();
        System.out.println("*************************Explain*************************");
        employeeDF.explain();
        System.out.println("*************************Explain*************************");
        employeeDF.explain(true);
        System.out.println("*************************Explain*************************");

        //2.清洗类算子
        Dataset<Row> dropGenderDF = employeeDF.drop("gender");
        dropGenderDF.show();

        Dataset<Row> distinctDF = employeeDF.distinct();
        distinctDF.show();

        Dataset<Row> dropDuplicatesDF = employeeDF.dropDuplicates();
        System.out.println("*************************DropDuplicates*************************");
        dropDuplicatesDF.show();
        System.out.println("*************************DropDuplicates*************************");
        Dataset<Row> genderDropDuplicatesDF = employeeDF.dropDuplicates("gender");
        System.out.println("*************************DropDuplicates*************************");
        genderDropDuplicatesDF.show();
        System.out.println("*************************DropDuplicates*************************");

        Dataset<Row> naFillGender = employeeDF.na().fill("Male", new String[] {"gender"});
        naFillGender.show();

        //3.转换算子
        employeeDF.select("name", "age").show();
        employeeDF.selectExpr("name", "age", "concat(id,'_',name) as id_name").show();

        employeeDF.where("age>=30").show();
        employeeDF.withColumnRenamed("gender", "sex").show();
        employeeDF.withColumn("age_1", employeeDF.col("age").plus(1)).show();

        //4.分析类算子
        Dataset<Row> fullInfoDF = employeeDF.join(salaryDF,
            employeeDF.col("id").equalTo(salaryDF.col("id")),
            "inner");
        Dataset<Row> aggDF = fullInfoDF.distinct().na().drop()
            .groupBy(col("gender"))
            .agg(sum(col("salary")).as("sum_salary"), avg(col("salary")).as("avg_salary"));

        aggDF.sort(desc("sum_salary"),asc("gender")).show();
        aggDF.orderBy(desc("sum_salary"),asc("gender")).show();

    }

}
