package com.pq.bigdata.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author qiong.peng
 * @Date 2019/11/4
 */
public class MyConsumer1 extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    private static final String ZOOKEEPER_ADDRESS = "basenode201:2181,basenode202:2181,basenode203:2181";

    public MyConsumer1(String topic) {
        ConsumerConfig consumerConfig = createConsumerCOnfig();
        consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        this.topic = topic;
    }

    private ConsumerConfig createConsumerCOnfig() {
        Properties prop = new Properties();
        prop.put("zookeeper.connect", ZOOKEEPER_ADDRESS);
        prop.put("group.id", "consumer1");
        prop.put("zookeeper.session.timeout.ms", "400");
        prop.put("auto.commit.interval.ms", "10000");
        // 当消费者第一次消费时，从最低的偏移量开始消费
        prop.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(prop);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumer.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> list = consumerMap.get(topic);

        KafkaStream stream0 = list.get(0);
        ConsumerIterator<byte[], byte[]> it = stream0.iterator();

        System.out.println("start...............");
        while(it.hasNext()){
            MessageAndMetadata<byte[], byte[]> value = it.next();
            int partition = value.partition();
            long offset = value.offset();
            String data = new String(value.message());
            System.err.println( data + " partition:" + partition + " offset:" + offset);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        MyConsumer1 consumer1 = new MyConsumer1("test");
        consumer1.start();
    }
}




















