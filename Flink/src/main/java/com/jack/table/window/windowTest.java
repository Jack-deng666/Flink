package com.jack.table.window;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class windowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String path = "F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\sensor.txt";
        // 注册表
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
                                .field("ts", DataTypes.BIGINT())
                                .rowtime(new Rowtime().timestampsFromField("ts")//表里面的事件时间定义
                                    .timestampsFromField("ts")   // 获取时间
                                    .watermarksPeriodicBounded(1000))// wasterMask
                                .field("temp",DataTypes.DOUBLE())
                                )
                            .createTemporaryTable("inputData");
        // 生成表
        Table inputData = tableEnv.from("inputData");
        inputData.printSchema();
        tableEnv.toAppendStream(inputData, Row.class).print();
        // table的window
//        Table tableWinData = inputData.window(Tumble.over("10.second").on("ts").as("tw"))
//                .groupBy("id, tw")
//                .select("id, id.count, temp.avg, tw.end");
//        // sql的window
//        //  1、注册mysql表
//        tableEnv.createTemporaryView("sensor",inputData);
//        Table sqlWinData = tableEnv.sqlQuery("select id, count(id) as cnt,avg(temp) as angTemp, tumble_end(rt,interval '10' second) from sensor " +
//                "group by id ,tumble(rt,interval '10' second)");
//
//        tableEnv.toAppendStream(tableWinData, Row.class).print("table_win");
//        tableEnv.toRetractStream(sqlWinData, Row.class).print("sql_win");
        env.execute();
    }
}
