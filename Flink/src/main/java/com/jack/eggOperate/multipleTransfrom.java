package com.jack.eggOperate;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Collections;

public class multipleTransfrom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.readTextFile("src/main/resources/seasor.txt");
        //数据类型转换
        SingleOutputStreamOperator<SensorReading> mapRusult = dataStream.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String value) throws Exception {
                String[] field = value.split(",");
                return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
            }
        });

        // 分流
        SplitStream<SensorReading> splitData = mapRusult.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTempperature() > 35.0) ? Collections.singletonList("hight") :
                        Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> low = splitData.select("low");
        DataStream<SensorReading> hight = splitData.select("hight");
        DataStream<SensorReading> select = splitData.select("low", "hight");
//
//        low.print("low");
//        hight.print("hight");
//        select.print("select");

        SingleOutputStreamOperator<Tuple2<String, Double>> newData = low.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {

            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTempperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = newData.connect(hight);

        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {

            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value._1(), value._2(), "低温预警");
            }

            @Override
            public Tuple3<String,Double,  String> map2(SensorReading value) throws Exception {
                return new Tuple3<>(value.getId(),value.getTempperature(),  "高温预警");
            }
        });

        map.print();
        env.execute();
    }
}
