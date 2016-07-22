package com.gmcc.hdp.demo.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * Created by $wally on 2016/7/21.
 */
public class KafkaKrbProducerDemo {

    public static void main(String[] args) {
        String topic = args[1];

        Properties props = new Properties();
        props.put("metadata.broker.list", args[0]);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("security.protocol", "PLAINTEXTSASL");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer<String, String>(config);

        for (int i = 0; i < 1000; i++) {
            producer.send(new KeyedMessage<String, String>(topic, "生产第"+i+"条数据的时间: " + new Date()));
        }
        producer.close();
    }
}
