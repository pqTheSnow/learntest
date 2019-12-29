/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shsxt.kafka.api;

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


//High level consumer API
public class MyConsumer2 extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public MyConsumer2(String topic) {
		consumer = Consumer
				.createJavaConsumerConnector(createConsumerConfig());

		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		//ZK地址
		props.put("zookeeper.connect", "basenode201:2181,basenode202:2181,basenode203:2181");
		//消费者所在组的名称
		props.put("group.id", "shsxt5");
        //ZK超时时间
		props.put("zookeeper.session.timeout.ms", "400");
		//消费者自动提交偏移量的时间间隔
		props.put("auto.commit.interval.ms", "1000");
        //当消费者第一次消费时，从最低的偏移量开始消费
		props.put("auto.offset.reset","smallest");
		//自动提交偏移量
        props.put("auto.commit.enable","true");

		return new ConsumerConfig(props);

	}

    /**
     * 多线程下，消费多个分区
     *
     * 当线程数<分区数：其中有的线程会消费多个分区上的数据
     * 当线程数=分区数: 一个线程对应一个分区上的数据
     * 当线程数>分区数: 多出来的线程会没有数据
     *
     * 注意：一个分区里的数据，只能被一个组里的一个线程消费，不能多个线程同时消费一个组里的数据
     *
     */
    public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, 2); // 描述读取哪个topic，需要几个线程读


		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

        // 每个线程对应于一个KafkaStream
        List<KafkaStream<byte[], byte[]>> list = consumerMap.get(topic);

        // 获取kafkastream流
        KafkaStream stream0 = list.get(0);
        KafkaStream stream1 = list.get(1);
//        KafkaStream stream2 = list.get(2);
//        KafkaStream stream3 = list.get(3);


        new Thread(new SubConsumer(stream0)).start();
        new Thread(new SubConsumer(stream1)).start();
//        new Thread(new SubConsumer(stream2)).start();
//        new Thread(new SubConsumer(stream3)).start();

	}

	public static void main(String[] args) {
		MyConsumer2 consumerThread = new MyConsumer2("test2");
		consumerThread.start();
	}
}



class SubConsumer implements Runnable{

    KafkaStream stream =null;
    public SubConsumer(KafkaStream stream){
        this.stream = stream;
    }

    @Override
    public void run() {

        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        System.out.println("start................");

        while (it.hasNext()){
            // 获取一条消息

            MessageAndMetadata<byte[], byte[]> value = it.next();
            int partition = value.partition();
            long offset = value.offset();
            String data = new String(value.message());

            System.err.println(Thread.currentThread() + " "+ data + " partition:" + partition + " offset:" + offset);

        }
    }
}
