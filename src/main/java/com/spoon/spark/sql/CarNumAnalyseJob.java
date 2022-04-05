package com.spoon.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.spoon.spark.sql.constant.SparkSqlConstant.CAR_NUM_APPLY;
import static com.spoon.spark.sql.constant.SparkSqlConstant.CAR_NUM_LUCKY;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sequence;

/**
 * @author: spoon
 * @email: szj94@qq.com
 * @create: 2022/4/5
 */
public class CarNumAnalyseJob {

    public static void main(String[] args) {
        //初始化
        SparkConf conf = new SparkConf().setAppName("Car Num Analyse").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        SparkSession session = new SparkSession(ctx.sc());

        Dataset<Row> applyDf = session.read().parquet(CAR_NUM_APPLY);
        //applyDf.show();
        Dataset<Row> luckyDf = session.read().parquet(CAR_NUM_LUCKY);
        //luckyDf.show();

        Dataset<Row> filterLuckyDf = luckyDf.filter(col("batchNum").$greater$eq("201601"))
            .select("carNum");
        //filterLuckyDf.show();
        Dataset<Row> joinDf = applyDf.join(filterLuckyDf,
            applyDf.col("carNum").equalTo(filterLuckyDf.col("carNum")),
            "inner")
            .select(applyDf.col("carNum").as("applyCarNum"),
                    applyDf.col("batchNum")
                );
        Dataset<Row> multipliers = joinDf.groupBy(col("applyCarNum"), col("batchNum"))
            .agg(count(lit(1)).as("multiplier"));
        //multipliers.show();

        Dataset<Row> uniqueMultipliers = multipliers.groupBy(col("applyCarNum"))
            .agg(max("multiplier").as("multiplier"));
        //uniqueMultipliers.show();

        Dataset<Row> result = uniqueMultipliers.groupBy("multiplier")
            .agg(count(lit(1)).as("cnt"))
            .orderBy(col("multiplier"));
        result.show();
    }

}
