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
public class MyConsumer3 extends Thread {
	private final ConsumerConnector consumer;
	private final String topic;

	public MyConsumer3(String topic) {
		consumer = Consumer
				.createJavaConsumerConnector(createConsumerConfig());

		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		//ZK地址
		props.put("zookeeper.connect", "basenode201:2181,basenode202:2181,basenode203:2181");
		//消费者所在组的名称
		props.put("group.id", "shsxt");
        //ZK超时时间
		props.put("zookeeper.session.timeout.ms", "400");
		//消费者自动提交偏移量的时间间隔
		props.put("auto.commit.interval.ms", "5000");
        //当消费者第一次消费时，从最低的偏移量开始消费
		props.put("auto.offset.reset","smallest");
		//自动提交偏移量
        props.put("auto.commit.enable","false");

		return new ConsumerConfig(props);

	}

    /**
     * 由自动提交偏移量而引发的数据重复消费和数据丢失问题
     * 1.重复消费：
     *      由于自动提交偏移量的周期时间过长
     *      若提交偏移量后，继续消费了一定的数据后，此时机器宕机
     *      但此时下次偏移量的提交时间还未到，那么重启进程后，会去zookeeper上读取上次提交的偏移量
     *      这就造成了数据重复消费
     * 2.数据丢失:
     *
     *  由于自动提交偏移量的周期时间过快，处理一条数据的时间过久
     *      客户端通过zookeeper上的偏移量去kafka中读取数据
     *      客户端将读取到的数据进行处理，在处理未完成之时
     *      提交偏移量的周期时间到了，comsumer就提交该条数据的偏移量到zk上
     *      此时忽然宕机，下次重启进程后，kafka就会去zk上读取偏移量，然后继续往后消费
     *      那么上次未处理完的那条数据就丢失了
     *
     * 解决方案：
     *    解决这两种情况的方案都是关闭偏移量自动提交，改成手动提交.
     *    props.put("auto.commit.enable","false");
     */
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

            String tmp = data + " partition:" + partition + " offset:" + offset;

            System.err.println( "开始处理数据: " + tmp);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.err.println( "数据处理中: " + tmp);
//
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//
            System.err.println( "数据处理完毕: " + tmp);
            //每条数据处理成功后，手动提交偏移量
            consumer.commitOffsets();
        }
			
	}

	public static void main(String[] args) {
		MyConsumer3 consumerThread = new MyConsumer3("test10");
		consumerThread.start();
	}
}

