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
package com.sxt.storm.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ManualDRPC {

	public static class ExclamationBolt extends BaseBasicBolt {

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("result", "return-info"));
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			//客户的数据下标为0,LinearDRPCTopologyBuilder 的下标为1，获取数据的时候需要主要区别。
			String arg = tuple.getString(0);
			Object retInfo = tuple.getValue(1);
			System.err.println(retInfo);
			collector.emit(new Values(arg + "!!!9@$^!@$^%#", retInfo));
		}

	}



	public static void main(String[] args) {


		TopologyBuilder builder = new TopologyBuilder();
		LocalDRPC drpc = new LocalDRPC();
		DRPCSpout spout = new DRPCSpout("exclamation", drpc);

		builder.setSpout("drpc", spout);
		builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
		builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");


		Config conf = new Config();

		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("exclaim", conf, builder.createTopology());



		System.err.println(drpc.execute("exclamation", "aaa"));


		System.err.println(drpc.execute("exclamation", "bbb"));

	}
}
