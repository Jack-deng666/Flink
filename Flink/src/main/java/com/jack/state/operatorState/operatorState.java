package com.jack.state.operatorState;

import com.jack.apiest.beans.SensorReading;
import com.jack.apiest.source.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

public class operatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<SensorReading> inputData = env.addSource(
                new SensorSource(1000,1));

        SingleOutputStreamOperator<Integer> mapSate = inputData.map(new MyOperatorState());
        mapSate.print("传出温度个数");
        env.execute();

    }

    public  static class MyOperatorState implements MapFunction<SensorReading, Integer>,
            ListCheckpointed<Integer> {
        // 初始状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }
//        设置检查点
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for(Integer num: state){
                count+=num;
            }
        }
    }
}
