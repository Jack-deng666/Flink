package com.jack.connect;

import java.util.Properties;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 16:56
 */
public class ConnectDescribe {
    public static Properties getKafkaProperties(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.0.22:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");
        return properties;
    }

    public static String getFileProperties(){
        return "F:\\LoadPinnacle\\Flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv";
    }
}
