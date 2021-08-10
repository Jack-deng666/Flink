package com.jack.state;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateSet {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置状态后端
        env.setStateBackend( new MemoryStateBackend());
        env.setStateBackend( new FsStateBackend(""));

        // 设置检查点  设置检查点间隔 和检查点模式
        env.enableCheckpointing(5000L);

        // 高级设置
        env.getCheckpointConfig().setCheckpointingMode(CheckpointConfig.DEFAULT_MODE);
        env.getCheckpointConfig().setCheckpointTimeout(60000l);
        /**
         * 等等
         */
        // 重启策略
        // 规定延迟时间 参数 重启次数 两次重启延迟时间
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,500L));
        // 失败率重启
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        DataStreamSource<String> inputData = env.readTextFile("src/main/resources/seasor.txt");

        SingleOutputStreamOperator<Object> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        env.execute();



    }
}
