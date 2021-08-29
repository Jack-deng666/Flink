package com.jack.marketanalysis;

import com.jack.beans.AdClickBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

import java.net.URL;

public class AdClick {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        URL resource = AdClick.class.getResource("/AdClickLog");
        DataStreamSource<String> inputData = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<AdClickBehavior> dataStream = inputData.map(line -> {
            String[] split = line.split(",");
            return new AdClickBehavior(new Long(split[0]), new Long(split[1]), split[2], split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickBehavior>() {
            @Override
            public long extractAscendingTimestamp(AdClickBehavior element) {
                return element.getTimestamp()*1000L;
            }
        });

        dataStream.keyBy(new KeySelector<AdClickBehavior, AdClickBehavior>() {
            @Override
            public AdClickBehavior getKey(AdClickBehavior value) throws Exception {
                return null;
            }
        })
                .process(new MyProFun(100))

    }

    public static class MyProFun extends KeyedProcessFunction<String ,AdClickBehavior,AdClickBehavior>{

        private Integer superBound;

        public MyProFun(Integer superBound) {
            this.superBound = superBound;
        }

        @Override
        public void processElement(AdClickBehavior value, Context ctx, Collector<AdClickBehavior> out) throws Exception {

        }

    }
}
