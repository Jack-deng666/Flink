package com.jack.processFunction;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 用processfunction实现分流
 */
public class processTempSide {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> inputData = env.socketTextStream("192.168.254.129", 50000);
        SingleOutputStreamOperator<SensorReading> dataStream = inputData.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] field = value.split(",");
                return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
            }
        });
        SingleOutputStreamOperator<SensorReading> dataStreamLambda = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });
        // 低温测流
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("low-Temp"){

        };
        OutputTag<SensorReading> outputTag1 = new OutputTag<SensorReading>("middle-Temp"){

        };
        SingleOutputStreamOperator<SensorReading> processData = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                if (value.gettemperature() > 30) {
                    out.collect(value);
                } else if(value.gettemperature() < 20) {
                    ctx.output(outputTag, value);
                }
                else{
                    ctx.output(outputTag1, value);
                }
            }
        });
        processData.print("high-temp");
        processData.getSideOutput(outputTag).print("low-temp");
        processData.getSideOutput(outputTag1).print("middle-temp");
        env.execute();
    }
}
