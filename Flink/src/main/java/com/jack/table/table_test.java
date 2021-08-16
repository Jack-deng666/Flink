package com.jack.table;

import com.jack.apiest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
 import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/11 19:37
 */

/**
 * 通过流处理创建表
 */
public class table_test {
    public static void main(String[] args) throws Exception {
        //  引入流失处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        // 读取数据
        DataStreamSource<String> inputData = env.readTextFile("F:\\LoadPinnacle\\Flink\\Flink\\src\\main\\resources\\sensor.txt");
        // 将数据打包成指定格式
        SingleOutputStreamOperator<SensorReading> dataStream = inputData.map(line -> {
            String[] field = line.split(",");
            return new SensorReading(field[0], new Long(field[1]), new Double(field[2]));
        });

        // 设置table环境
        StreamTableEnvironment TableEnv = StreamTableEnvironment.create(env);
        // 创建一张表
        Table tableData = TableEnv.fromDataStream(dataStream);

        // 对表进行操作
        Table tableResult1 = tableData.select("id, temperature").where("id='sensor_1'");
        // 执行sql
        TableEnv.createTemporaryView("sensor", dataStream);
        String sql = "select id, temperature from sensor where id='sensor_2'";
        Table tableResult2 = TableEnv.sqlQuery(sql);
        // 将数据再转成流数据 在输出
        TableEnv.toAppendStream(tableResult1, Row.class).print("table");
        TableEnv.toAppendStream(tableResult2, Row.class).print("sql");

        env.execute();
    }
}
