package com.jack.window;

import com.jack.apiest.beans.SensorReading;
import com.jack.apiest.source.sourceTest3_udf;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import sun.awt.util.IdentityArrayList;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/7/27 21:01
 */
public class WindowFirst {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        DataStreamSource<String> inputData = env.socketTextStream("10.0.0.22", 5203);
//        DataStream<SensorReading> dataStream = inputData.map(line -> {
//            String[] field = line.split(",");
//            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
//        });
        DataStreamSource<SensorReading> dataStream = env.addSource(new sourceTest3_udf.MySensorSource());
        //1、 增量窗口
        SingleOutputStreamOperator<Integer> resultWindow = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
//        dataStream.print("data");
        // 2、 全窗口函数

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> id = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input,
                                                        Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long WindowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, WindowEnd, count));
                    }
                });
        id.print();
//        resultWindow.print("count");
        env.execute();
    }
}
