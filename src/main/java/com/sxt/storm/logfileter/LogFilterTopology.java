/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sxt.storm.logfileter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class LogFilterTopology {

	public static class FilterBolt extends BaseBasicBolt {
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String line = tuple.getString(0);
			System.out.println("Accept:  " + line);
			// 包含ERROR的行留下
			if (line.contains("ERROR")) {
				System.err.println("Filterbolt:  contain ERROR.. emit.." + line);
				collector.emit(new Values(line));
			}

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// 定义message提供给后面FieldNameBasedTupleToKafkaMapper使用
			declarer.declare(new Fields("message"));
		}


    }

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// config kafka spout，话题
		String topic = "fks";
        /**
         * brokerZkStr : kafka集群所使用的zookeeper集群地址
         * 通过kafka的zk地址也可以连接到kafka集群，通过zk上的 /broker目录
         */
        // 从zk中读数据-配置zk地址
		ZkHosts zkHosts = new ZkHosts("basenode201:2181,basenode202:2181,basenode203:2181");
        /**
         *  /kafka_storm: 偏移量offset的根目录
         *  fks: 在偏移量根目录 /kafka_storm下，创建子目录fks,来记录程序消费到哪个偏移量。
         *  */
		// kafka集群配置，数据源在哪个topic
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/kafka_storm", "fks");// 对应一个应用

        System.out.println(zkHosts.brokerZkStr);

        //保存消费者偏移量的zk地址,可以是另外一个zk集群
		spoutConfig.zkServers = Arrays.asList("basenode201", "basenode202", "basenode203");
		spoutConfig.zkPort = 2181;

		// StringScheme将字节流转解码成某种编码的字符串
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		// set kafka spout
		builder.setSpout("kafka_spout", kafkaSpout, 1);

		// set bolt
		builder.setBolt("filter", new FilterBolt(), 1).shuffleGrouping("kafka_spout");

		// 数据写出
		// set kafka bolt
		// withTopicSelector使用缺省的选择器指定写入的topic： LogError
		// 定义卡发卡的bolt，将数据输出到kafka的那个topic
		KafkaBolt kafka_bolt = new KafkaBolt().withTopicSelector(new DefaultTopicSelector("Log_error"));
		// 最终结果处理的bolt，将结果输出到第三方，比如DB，kafka等
		builder.setBolt("kafka_bolt", kafka_bolt, 1).shuffleGrouping("filter");

		Config conf = new Config();
		// set producer properties. 定义bolt输出的kafka的相关信息，比如集群地址
		Properties props = new Properties();
		props.put("metadata.broker.list", "basenode201:9092,basenode202:9092,basenode203:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put("kafka.broker.properties", props);

		// 本地方式运行
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("mytopology", conf, builder.createTopology());
		System.err.println("====================haha=======================");

	}
}
