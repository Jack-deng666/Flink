package com.jack.read;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/7/30 11:58
 */
public class hdfs_read {
    public static void main(String[] args) {
        Dataset<Row> dataResult = SparkSession.builder().master("local[*]")
                .getOrCreate()
                .read()
                .text("hdfs://host1:8020/tmp/pile_charge_data1/part-m-00004");

        dataResult.limit(4).foreach((ForeachFunction<Row>) x->System.out.println(x));
//        Row[] take = dataResult.take(5);
//        System.out.println(take);
//        dataResult.show(5);
    }
}
