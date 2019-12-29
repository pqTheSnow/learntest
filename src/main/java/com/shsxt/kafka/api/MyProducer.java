package com.shsxt.kafka.api;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;


/**
 * 向kafka中生产数据
 *
 * @author root
 */
public class MyProducer extends Thread {

    private String topic; //发送给Kafka的数据,topic
    private Producer<String, String> producerForKafka;

    public MyProducer(String topic) {

        this.topic = topic;

        Properties conf = new Properties();

        conf.put("metadata.broker.list", "basenode201:9092,basenode202:9092,basenode203:9092");

        conf.put("serializer.class", StringEncoder.class.getName());

        /**
         * ack=0 生产者不会等待来自任何服务器的响应,一直发送数据
         * ack=1 leader收到数据后，给生产者返回响应消息，生产者再继续发送新的数据
         * ack=all 生产者发送一条数据后，leader会等待所有isr列表里的服务器同步好数据后，才返回响应。
         *
         * ack=0.吞吐量高，但是消息存在丢失风险。
         * ack=1.数据的安全性和性能 都有一定保障
         * ack=all 安全性最高，但性能最差
         */

        conf.put("acks",1);

        //缓存数据，批量发送，当需要发送到同一个partition中的数据大小达到15KB时，将数据发送出去
        conf.put("batch.size", 16384);

        producerForKafka = new Producer<>(new ProducerConfig(conf));
    }


    @Override
    public void run() {
        int counter = 0;

        while (true) {


            String value = "shsxt" + counter;

            String key = counter + "";
            /**
             * producer将 message发送数据到 kafka topic的时候，这条数据应该发到哪个partition分区里呢？
             *   message 有key,value组成
             *   当message的key为null值，则将message随机发送到partition里
             *   当message的key不为null值时，则通过key 取hash后 ，对partition_number 取余数，得到数就是partition id.
             */

//            KeyedMessage<String, String> message = new KeyedMessage<>(topic,value);
            KeyedMessage<String, String> message = new KeyedMessage<>(topic, key,value);

            producerForKafka.send(message);

            System.out.println(value + " - -- -- --- -- - -- - -");

            //每2条数据暂停1秒
            if (0 == counter % 2) {

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            counter++;
        }
    }

    public static void main(String[] args) {

        new MyProducer("test").start();

    }

}
