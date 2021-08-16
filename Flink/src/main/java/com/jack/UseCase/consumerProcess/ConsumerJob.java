package com.jack.UseCase.consumerProcess;

import org.apache.flink.table.api.*;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/16 18:21
 */
public class ConsumerJob {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 连接kafka  注册表
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("dengjirui")
                .property("zookeeper.connect", "host1:2181")
                .property("bootstrap.server","host1:9092"))
                .withFormat(new Csv()
                ).withSchema(new Schema()
                .field("accountId", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("amount", DataTypes.DOUBLE())
        ).createTemporaryTable("inputData");
        //生成表
        Table inputData = tableEnv.from("inputData");


        tableEnv.execute("CREATE TABLE send_report (" +
                "account_id BIGINT," +
                "ts TIMESTAMP(3)," +
                "amount decimal(10,5)," +
                "primary key (account_id,ts) NOT ENFORCED)" +
                "WHIT (" +
                "'connect' = 'jdbc')," +
                "'url'     = 'jdbc:mysql://localhost:3306/mydb?useSSL=false'," +
                "'table_name' = 'spend_report'," +
                "'driver'  = 'com.mysql.jdbc.Driver'," +
                "'username'= 'root'." +
                "'password'= 'root')");

        Table filter = inputData.select("account_id,ts,amount").filter("amount<0.2");
//        filter.execute("send_report");
kafka

    }

}
