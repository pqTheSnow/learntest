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

/**
 * Kafka消费者API分为两种
 * 1.High level consumer API
 *  此种API，偏移量由zookeeper来保存，使用简单，但是不灵活
 * 2.Simple level consumer API
 *  此种API，不依赖Zookeeper，无论从自由度和性能上都有更好的表现，但是开发更复杂
 *
 *
 *     topic下的一个partition分区，只能被同一组下的一个消费者消费。
 * 	要想保证消费者从topic中消费的数据是有序的，则应当将topic的分区设置为1个partition
 *
 */
//High level consumer API
public class MyConsumer1 extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public MyConsumer1(String topic) {

        ConsumerConfig consumerConfig = createConsumerConfig();
        consumer = Consumer
				.createJavaConsumerConnector(consumerConfig);

		this.topic = topic;
	}


	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		//ZK地址
		props.put("zookeeper.connect", "basenode201:2181,basenode202:2181,basenode203:2181");
		//消费者所在组的名称
		props.put("group.id", "shsxt3");
        //ZK超时时间
		props.put("zookeeper.session.timeout.ms", "400");
		//消费者自动提交偏移量的时间间隔
		props.put("auto.commit.interval.ms", "10000");
        //当消费者第一次消费时，从最低的偏移量开始消费
		props.put("auto.offset.reset","smallest");
		//自动提交偏移量
//        props.put("auto.commit.enable","true");

		return new ConsumerConfig(props);

	}


	public void run() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		topicCountMap.put(topic, 1); // 描述读取哪个topic，需要几个线程读

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

        // 每个线程对应于一个KafkaStream
        List<KafkaStream<byte[], byte[]>> list = consumerMap.get(topic);

        // 获取kafkastream流
        KafkaStream stream0 = list.get(0);

		ConsumerIterator<byte[], byte[]> it = stream0.iterator();

        System.out.println("start................");

        while (it.hasNext()){
            // 获取一条消息

            MessageAndMetadata<byte[], byte[]> value = it.next();

            int partition = value.partition();

            long offset = value.offset();

            String data = new String(value.message());

            System.err.println( data + " partition:" + partition + " offset:" + offset);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
			
	}

	public static void main(String[] args) {
		MyConsumer1 consumerThread = new MyConsumer1("test");
		consumerThread.start();
	}
}

