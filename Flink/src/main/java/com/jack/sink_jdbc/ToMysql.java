package com.jack.sink_jdbc;

import com.jack.apiest.beans.SensorReading;
import com.jack.apiest.source.sourceTest3_udf;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/7/27 19:51
 */
public class ToMysql {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<SensorReading> dataStream = env.addSource(new sourceTest3_udf.MySensorSource());
        dataStream.addSink( new MyJdbcSink());
        dataStream.print();
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {
        Connection connection = null;
        PreparedStatement insertStatus = null;
        PreparedStatement updateStatus = null;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb","root","root");
            insertStatus = connection.prepareStatement("insert into sensor_temp (id, temperature) values (?,?)");
            updateStatus = connection.prepareStatement("update sensor_temp set temperature = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStatus.setDouble(1,value.gettemperature());
            updateStatus.setString(2,value.getId());
            updateStatus.execute();
            if (updateStatus.getUpdateCount() == 0){
                insertStatus.setString(1,value.getId());
                insertStatus.setDouble(2,value.gettemperature());
                insertStatus.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatus.close();
            updateStatus.close();
            connection.close();
        }
    }
}
