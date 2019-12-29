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
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;


/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This
 * program creates a topology that can compute the reach for any URL on Twitter
 * in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute
 * reach, you have to get all the people who tweeted the URL, get all the
 * followers of all those people, unique that set of followers, and then count
 * the unique set. It's an intense computation that can involve thousands of
 * database calls and tens of millions of follower records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning
 * what would be a computation that takes minutes on a single machine into one
 * that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual
 * DBs with in-memory hashmaps.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more
 * information on Distributed RPC.
 */
public class ReachTopology {

	public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
		{
			put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
			put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
			put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
		}
	};

	public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
		{
			put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
			put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
			put("tim", Arrays.asList("alex"));
			put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
			put("adam", Arrays.asList("david", "carissa"));
			put("mike", Arrays.asList("john", "bob"));
			put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
		}
	};

	public static class GetTweeters extends BaseBasicBolt {
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			
			Object id = tuple.getValue(0);
			String url = tuple.getString(1);
			List<String> tweeters = TWEETERS_DB.get(url);
			if (tweeters != null) {
		
					for (String tweeter : tweeters) {
						collector.emit(new Values(id, tweeter));
					}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "tweeter"));
		}
	}

	public static class GetFollowers extends BaseBasicBolt {
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			Object id = tuple.getValue(0);
			String tweeter = tuple.getString(1);
			List<String> followers = FOLLOWERS_DB.get(tweeter);
			if (followers != null) {
				for (String follower : followers) {
					collector.emit(new Values(id, follower));
				}
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "follower"));
		}
	}

	public static class PartialUniquer extends BaseBatchBolt {
		BatchOutputCollector _collector;
		Object _id;
		int count = 0;
		Set<String> _followers = new HashSet<String>();

		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
//			System.err.println(this);
//			System.err.println(Thread.currentThread().getName()+"-"+ Thread.currentThread().getId()+"   id  :"+_id +"   PartialUniquer初始化啦====");
		}

		@Override
		public void execute(Tuple tuple) {
//			System.err.println(Thread.currentThread().getName()+"-"+ Thread.currentThread().getId()+"   count  :"+(count++) +"   PartialUniquer执行excutte啦====");
            _followers.add(tuple.getString(1));

		}
		//批处理，本批的数据excute都调用完之后，执行finishBatch方法
		@Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _followers.size()));
//            System.err.println(Thread.currentThread().getName() +"-"+ Thread.currentThread().getId()+ "   id  :"+_id +"   PartialUniquer执行batch啦");
        }

        @Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "partial-count"));
		}
	}

	public static class CountAggregator extends BaseBatchBolt {
		BatchOutputCollector _collector;
		Object _id;
		int _count = 0;

		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
//			System.err.println(Thread.currentThread().getName()+"-"+ Thread.currentThread().getId()+"   CountAggregator—id  :"+_id +"   初始化啦====");
		}

		@Override
		public void execute(Tuple tuple) {
			_count += tuple.getInteger(1);
//			System.err.println(Thread.currentThread().getName()+"-"+ Thread.currentThread().getId()+"   CountAggregator--excute---count值：  :"+_count );
		}

		@Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
//            System.err.println(Thread.currentThread().getName()+"-"+ Thread.currentThread().getId()+"  执行批量 ：  CountAggregator---finish----count值  :"+_count );

        }

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "reach"));
		}
	}

	public static LinearDRPCTopologyBuilder construct() {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
		builder.addBolt(new GetTweeters(), 1);//用户需要统计的url有哪些人转发了。
		builder.addBolt(new GetFollowers(), 3).shuffleGrouping();//转发了url的人的粉丝
		builder.addBolt(new PartialUniquer(), 3).fieldsGrouping(new Fields("id", "follower"));//局部去重
		builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id"));//求和，算出总的受众人数。
				
		return builder;
	}

	public static void main(String[] args) throws Exception {
		
		LinearDRPCTopologyBuilder builder = construct();

		Config conf = new Config();

		if (args == null || args.length == 0) {
			conf.setMaxTaskParallelism(6);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));

			String[] urlsToTry = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
			for (String url : urlsToTry) {
				System.err.println("Reach of " + url + ":   " + drpc.execute("reach", url));
			}
	
			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createRemoteTopology());
		}
	}
}
