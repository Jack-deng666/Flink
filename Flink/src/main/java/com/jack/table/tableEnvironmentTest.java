package com.jack.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * 直接创建表的环境
 */
public class tableEnvironmentTest {
    public static void main(String[] args) {
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
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkEnvStreamSettings);

        // 1.4 新版本的blink批处理环境
        EnvironmentSettings blinkEnvBatchSettings = EnvironmentSettings.
                newInstance().
                useBlinkPlanner().
                inBatchMode().
                build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkEnvBatchSettings);
    }
}
