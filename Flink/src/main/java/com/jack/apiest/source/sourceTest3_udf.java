package com.jack.apiest.source;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class sourceTest3_udf {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();
        env.execute();


    }

    // 实现自定义数据

    public static  class MySensorSource implements SourceFunction<SensorReading> {
        private boolean Running = true;
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温服
            HashMap<String,Double> sensorTempMap = new HashMap<>();
            for(int i = 0;i<10;i++){
                sensorTempMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }

            while (Running){
                for(String sensorId:sensorTempMap.keySet()){
                    Double nexTemp = sensorTempMap.get(sensorId)+random.nextGaussian()*0.5;
                    sensorTempMap.put(sensorId, nexTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), nexTemp));
                }
                // 控制台输出等待1S
                Thread.sleep(1000L);


            }
        }

        @Override
        public void cancel() {
            Running = false;
        }
    }

}
