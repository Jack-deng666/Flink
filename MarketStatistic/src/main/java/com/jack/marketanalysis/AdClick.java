package com.jack.marketanalysis;

import com.jack.beans.AdClickBehavior;
import com.jack.beans.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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

        SingleOutputStreamOperator<AdClickBehavior> process = dataStream.keyBy(new KeySelector<AdClickBehavior, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickBehavior value) throws Exception {
                return new Tuple2<Long, Long>(value.getUserId(), value.getAdId());
            }
        })
                .process(new MyProFun(100));

        process
                .keyBy(AdClickBehavior::getProvince)
                .timeWindow(Time.hours(1),Time.seconds(5)).aggregate()

    }

    public static class MyProFun extends KeyedProcessFunction<Tuple2<Long, Long> ,AdClickBehavior,AdClickBehavior>{

        private Integer superBound;

        public MyProFun(Integer superBound) {
            this.superBound = superBound;
        }

        ValueState<Long> countState;
        ValueState<Boolean>  isSentSate;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-click", Long.class,0L));
            isSentSate = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("send-back-list", Boolean.class,false));
        }



        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(AdClickBehavior value, Context ctx, Collector<AdClickBehavior> out) throws Exception {
            // 判断当前用户对同一广告的点击次数，如果不够上限，该count加1正常输出；
            // 如果到达上限，直接过滤掉，并侧输出流输出黑名单报警

            Long countClick = countState.value();
            Boolean isSent = isSentSate.value();

            if(countClick==0){
                Long currentTime = System.currentTimeMillis();
                Long ts = (currentTime/(24*60*60*1000)+1)*(24*60*60*1000);
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            if(countClick>=superBound){
                if(isSent!=true){
                    isSentSate.update(true);
                    ctx.output(new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(value.getUserId(), value.getAdId(), "click over " + superBound + "times."));
                }
                return;
            }
            // 没出现阈值
            countState.update(countClick+1);
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickBehavior> out) throws Exception {
            countState.clear();
            isSentSate.clear();
        }
    }
}
