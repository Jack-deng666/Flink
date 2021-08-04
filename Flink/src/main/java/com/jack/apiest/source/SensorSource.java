package com.jack.apiest.source;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public  class SensorSource implements SourceFunction<SensorReading> {
    public Integer timeSleep = 0;
    public int sensorNum = 10;

    public SensorSource(Integer timeSleep, int sensorNum) {
        this.timeSleep = timeSleep;
        this.sensorNum = sensorNum;
    }

    public SensorSource(Integer timeSleep) {
        this.timeSleep = timeSleep;
    }

    private boolean Running = true;
    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        // 定义一个随机数发生器
        Random random = new Random();

        // 设置10个传感器的初始温服
        HashMap<String,Double> sensorTempMap = new HashMap<>();
        for(int i = 0;i<sensorNum;i++){
            sensorTempMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
        }

        while (Running){
            for(String sensorId:sensorTempMap.keySet()){
                Double nexTemp = sensorTempMap.get(sensorId)+random.nextGaussian()*0.5;
                sensorTempMap.put(sensorId, nexTemp);
                ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), nexTemp));
            }
            // 控制台输出等待1S
            Thread.sleep(timeSleep);


        }
    }

    @Override
    public void cancel() {
        Running = false;
    }
}
