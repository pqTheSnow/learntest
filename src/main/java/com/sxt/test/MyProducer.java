package com.sxt.test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;
import java.util.Random;

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

        producerForKafka = new Producer<>(new ProducerConfig(conf));
    }


    @Override
    public void run() {

        int count = 0;

        while(true) {
            int number = new Random().nextInt(3);
            String sampleData ;

            if(number == 0){
                sampleData  = "Hello Flume! ERROR   " + count;
            }else if(number==1){
                sampleData  = "Hello Flume! INFO   " + count;
            }else {
                sampleData  = "Hello Flume! WARNING   " + count;
            }

            KeyedMessage<String, String> message = new KeyedMessage<>(topic, count+"",sampleData);

            producerForKafka.send(message);

            System.out.println(sampleData + " - -- -- --- -- - -- - -");

            //每2条数据暂停1秒
            if (0 == count % 2) {

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

        new MyProducer("fks").start();
    }

}
