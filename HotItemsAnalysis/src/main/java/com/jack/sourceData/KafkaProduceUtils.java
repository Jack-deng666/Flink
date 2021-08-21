package com.jack.sourceData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/21 17:46
 */
public class KafkaProduceUtils {
    public static void main(String[] args) throws Exception {
        WriteToKafka("dengjirui");
    }

    public static void  WriteToKafka(String topic) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.0.0.22:9092");
        properties.setProperty("key.serializer",   "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        BufferedReader bufferedReader =
                new BufferedReader(new FileReader("F:\\LoadPinnacle\\Flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line;
        while((line = bufferedReader.readLine())!=null){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);

        }
        kafkaProducer.close();

    }
}
