package com.gmcc.hdp.demo.kafka;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by $wally on 2016/7/21.
 */
public class KafkaKrbConsumerDemo {

    public static void main(String[] args) {

        String topic="jim";
        Properties properties = new Properties();

        //依赖的Zookeeper服务器地址
        properties.put("zookeeper.connect", HDPSampleConfiguration.KAFKA_ZOOKEEPER_CONNECT_LIST);
        //consumer对象所属的group.id
        properties.put("group.id", HDPSampleConfiguration.KAFKA_GROUP_ID);

        ConsumerConnector consumer= Consumer.createJavaConsumerConnector(new ConsumerConfig(properties) );

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topicMap);
        KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        System.out.println("---开始消费数据---");
        while (it.hasNext()) {
            System.err.println("消费数据: " + new String(it.next().message()));
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
