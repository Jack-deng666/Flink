package com.jack.window;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/7/27 21:01
 */
public class WindowFirst {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputData = env.socketTextStream("10.0.0.22", 5203);
        DataStream<SensorReading> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
//        DataStreamSource<SensorReading> dataStream = env.addSource(new sourceTest3_udf.MySensorSource());

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
        resultWindow.print("count");
        env.execute();
    }
}
