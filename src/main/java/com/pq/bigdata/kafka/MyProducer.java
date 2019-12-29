package com.pq.bigdata.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * @author qiong.peng
 * @Date 2019/11/4
 */
public class MyProducer extends Thread {

    // 发送给kafka的数据的topic
    private String topic;
    private Producer<String, String> producerForKafka;

    private static final String KAFKA_BROKERS = "basenode201:9092,basenode202:9092,basenode203:9092";

    public MyProducer(String topic) {
        this.topic = topic;
        Properties conf = new Properties();
        conf.put("metadata.broker.list", KAFKA_BROKERS);
        conf.put("serializer.class", StringEncoder.class.getName());
        conf.put("acks", 1);
        // 缓存数据，批量发送，当需要发送到同一个partition中的数据大小达到15KB时，将数据发送出去
        conf.put("batch.size", 16384);

        producerForKafka = new Producer<>(new ProducerConfig(conf));
    }

    @Override
    public void run() {
        int count = 0;
        while(true){
            String value = "君子藏器於身，待时而动" + count;
            String key = String.valueOf(count);
            KeyedMessage<String, String> message = new KeyedMessage<>(topic, key, value);
            producerForKafka.send(message);
            System.out.println("--------------------");
            if(count % 2 == 0){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            count++;
        }
    }

    public static void main(String[] args) {
        new MyProducer("test").start();
    }
}








































