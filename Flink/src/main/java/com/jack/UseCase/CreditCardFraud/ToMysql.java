package com.jack.UseCase.CreditCardFraud;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 16:06
 */
public class ToMysql extends RichSinkFunction<ConsumerMark> {
    Connection connection = null;
    PreparedStatement insertData = null;


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?useSSL=false","root","root");
        insertData = connection.prepareStatement("insert into " +
                "fraud_detector (consumerId, amount, ts, normal_consumer) values (?,?,?,?)");
    }

    @Override
    public void close() throws Exception {
        insertData.close();
        connection.close();
    }

    @Override
    public void invoke(ConsumerMark value, Context context) throws Exception {
        insertData.setString(1,value.getConsumerInfo());
        insertData.setDouble(2, value.getAmount());
        insertData.setLong(3, value.getTs());
        insertData.setString(4,String.valueOf(value.isNormal()));
        insertData.execute();
    }
}
