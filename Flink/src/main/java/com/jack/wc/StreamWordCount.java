package com.jack.wc;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(8); //设置并行度
        // 读取本地数据

        /*
        String inputPath = "D:\\java_project\\Flink_Demo\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> streamData = env.readTextFile(inputPath);
         */

        /*开启端口监控
            在linux开启端口:
                        nc -lk 7777
        */
        // 动态获取配置信息
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        //获取端口数据流
        DataStreamSource<String> streamData = env.socketTextStream(host, port);

        // 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = streamData.flatMap(new WordCount.myFlatMapper())
                .keyBy(0)
                .sum(1).setParallelism(2);
        resultStream.print().setParallelism(1);
        env.execute();
    }
}
