package com.jack.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * 直接创建表的环境
 */
public class tableEnvironmentTest {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 基于老版本的planner的流处理
        EnvironmentSettings oldEnvStreamSettings = EnvironmentSettings.
                newInstance().
                useOldPlanner().
                inStreamingMode().
                build();
        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldEnvStreamSettings);

        //1.2 老版本的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        //1.3  新版本的blink流处理
        EnvironmentSettings blinkEnvStreamSettings = EnvironmentSettings.
                newInstance().
                useBlinkPlanner().
                inStreamingMode().
                build();
//        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkEnvStreamSettings);

        // 1.4 新版本的blink批处理环境
        EnvironmentSettings blinkEnvBatchSettings = EnvironmentSettings.
                newInstance().
                useBlinkPlanner().
                inBatchMode().
                build();
//        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkEnvBatchSettings);
        String path = "F:\\RoadPinnacle\\Flink\\Flink\\Flink\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(path))
                .withFormat(new Csv())
                .withSchema(new Schema().
                        field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())).createTemporaryTable("inputData");

        Table inputData = tableEnv.from("inputData");
//        inputData.printSchema();
//        tableEnv.toAppendStream(inputData, Row.class).print();
        Table filterResult = inputData.select("id,temp").filter("id='sensor_3'");
        Table groupResult = inputData.groupBy("id").select("id, id.count as cnt, temp.avg as temp__avg");
        Table sqlGroupResult = tableEnv.sqlQuery("select id, count(id) cnt, avg(temp) avg_temp from inputData group by id");
        tableEnv.toAppendStream(filterResult, Row.class).print("filterResult");
        tableEnv.toRetractStream(groupResult, Row.class).print("table_egg");
        tableEnv.toRetractStream(sqlGroupResult, Row.class).print("sqlGroupResult");




        env.execute();
    }
}
