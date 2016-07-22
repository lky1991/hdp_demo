package com.gmcc.hdp.demo.kafka;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 向kafka队列中生产数据、消费数据
 * Created by $wally on 2016/7/11.
 */
public class KafkaDemo {

    private Producer<String, String> producer = null;
    private ConsumerConnector consumer = null;
    private static Integer messageNum = 100;


    public KafkaDemo() {
        this.producer = new Producer<String, String>(createProducerConfig());
        this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    /**
     * 创建Producer的配置文件
     *
     * @return
     */
    public ProducerConfig createProducerConfig() {
        Properties properties = new Properties();

        //broker列表地址
        properties.put("metadata.broker.list", HDPSampleConfiguration.KAFKA_BROKER_LIST);

        //指定message的序列化编码类（该编码类不是固定不变的，可以根据不同的业务需求来定制开发message的序列化编码类）
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        return new ProducerConfig(properties);
    }


    /**
     * 创建Consumer的配置文件
     *
     * @return
     */
    public ConsumerConfig createConsumerConfig() {
        Properties properties = new Properties();

        //依赖的Zookeeper服务器地址
        properties.put("zookeeper.connect", HDPSampleConfiguration.KAFKA_ZOOKEEPER_CONNECT_LIST);

        //consumer对象所属的group.id
        properties.put("group.id", HDPSampleConfiguration.KAFKA_GROUP_ID);

        return new ConsumerConfig(properties);
    }

    /**
     * Producer 生产数据
     */
    public void producerData(String topic) {
        for (int i = 1; i <= messageNum; ++i) {
            String messageStr = new String("Message_" + i);
            System.err.println("生产第" + i + "条数据：" + messageStr);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, messageStr);
            producer.send(data);
        }
        producer.close();
    }

    /**
     * Consumer 消费数据
     */
    public void consumerData(String topic) {
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
