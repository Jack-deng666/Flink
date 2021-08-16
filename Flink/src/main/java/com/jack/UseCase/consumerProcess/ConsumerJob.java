package com.jack.UseCase.consumerProcess;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
        // 连接kafka  注册表
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("dengjirui")
                .property("zookeeper.connect", "host1:2181")
                .property("bootstrap.server","host1:9092"))
                .withFormat(new Csv()
                ).withSchema(new Schema()
                .field("account_id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("amount", DataTypes.DOUBLE())
        ).createTemporaryTable("inputData");
        //生成表
        Table inputData = tableEnv.from("inputData");


        tableEnv.sqlUpdate("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    ts     TIMESTAMP(3),\n" +
                "    amount     decimal(10,5)\n" +
                ") WITH (\n" +
                "    'connector'  = 'jdbc',\n" +
                "    'url'        = 'jdbc:mysql://mysql:3306/mydb',\n" +
                "    'table-name' = 'spend_report',\n" +
                "    'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "    'username'   = 'root',\n" +
                "    'password'   = 'root'\n" +
                ")");

        Table filter = inputData.select("account_id,ts,amount").filter("amount<0.2");
//        filter.execute("send_report");
        filter.insertInto("send_report");
        env.execute();

    }

}
