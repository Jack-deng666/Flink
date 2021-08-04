package com.jack.state.keydState;

import com.jack.apiest.beans.SensorReading;
import com.jack.apiest.source.SensorSource;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class KeydState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> inputData = env.addSource(
                new SensorSource(1000,1));

        SingleOutputStreamOperator<Integer> resultData = inputData.keyBy("id")
                .map(new MyMapFunction());
        resultData.print("keydState");
        env.execute();

    }
//自定义mapfuncton
    public static class MyMapFunction extends RichMapFunction<SensorReading,Integer>{
    // valueState用法
    private ValueState<Integer> keydStateCount;
    // 其他类型的State
    //listState
    private ListState<String> myListState;
    private MapState<String,Double> MyMapState;
    private ReducingState<SensorReading> myReduceState;


    @Override
    public Integer map(SensorReading value) throws Exception {


        // ListState
        Iterable<String> strings = myListState.get();
        for(String str: myListState.get()){

        }
        myListState.add("sensor_1");
        // mapState
        Iterable<String> keys = MyMapState.keys();
        MyMapState.get("sensor_1");

        // valuesState
        Integer count = keydStateCount.value();
        count++;
        keydStateCount.update(count);
        return count;
        // 其他State


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        keydStateCount = getRuntimeContext().getState(
                new ValueStateDescriptor<Integer>("key_count", Integer.class, 0));
        // listState
        myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list_count",String.class));
        // MapState
        MyMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<String, Double>("map_count",String.class, Double.class));
    }
}
}
