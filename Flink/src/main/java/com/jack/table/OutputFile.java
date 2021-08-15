package com.jack.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class OutputFile {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String path = "F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\sensor.txt";
        // 注册表
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema().
                        field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())).createTemporaryTable("inputData");
//        插入数据
        Table inputData = tableEnv.from("inputData");
//        inputData.printSchema();
        // 数据筛选
        Table filterResult = inputData.select("id,temp").filter("id='sensor_1'");
        tableEnv.toAppendStream(filterResult, Row.class).print("111");
//        执行
        tableEnv.connect(new FileSystem().path("F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\out.txt"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE())).createTemporaryTable("output");
        tableEnv.insertInto("output", filterResult);

        env.execute();

    }
}
