package com.jack.window;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.OutputStream;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/2 19:04
 */
public class WindowTime {
    public static void main(String[] args) throws Exception {
//
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义 --时间时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<String> inputStream = env.socketTextStream("10.0.0.39", 54321);
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        //设置乱序时间戳和watermask
        SingleOutputStreamOperator<SensorReading> timeDataStream = dataStream.
                assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.milliseconds(10)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });
        // 开窗

        OutputTag<SensorReading> lateData = new OutputTag<SensorReading>("lateData"){};
        SingleOutputStreamOperator<SensorReading> minTempStream = timeDataStream.keyBy("id")
                .timeWindow(Time.seconds(15)).
                        allowedLateness(Time.minutes(1)) //设置迟到时间 一分钟
                .sideOutputLateData(lateData)
                        .minBy("temperature");
        minTempStream.print("");
        minTempStream.getSideOutput(lateData).print("lata");
        env.execute();


    }
}
