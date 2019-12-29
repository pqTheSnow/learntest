package com.cmccstormjk02.topo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.cmccstormjk02.bolt.CellDaoltBolt;
import com.cmccstormjk02.bolt.CellFilterBolt;
import com.cmccstormjk02.cmcc.constant.Constants;
import com.cmccstormjk02.kafka.productor.KafkaProperties;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class KafkaOneCellMonintorTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		ZkHosts zkHosts = new ZkHosts(Constants.KAFKA_ZOOKEEPER_LIST);
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, 
				"cmcc_0508",
				"/kafka_storm", // 偏移量offset的根目录
				"cmcc"); // 对应一个应用

        //storm zk地址
		spoutConfig.zkServers = Arrays.asList("basenode201","basenode202","basenode203");
		spoutConfig.zkPort = 2181;

		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()); 

		builder.setSpout("spout", new KafkaSpout(spoutConfig),1);
		builder.setBolt("cellBolt", new CellFilterBolt(), 3).shuffleGrouping("spout");
		builder.setBolt("cellDaoltBolt", new CellDaoltBolt(), 5)
				.fieldsGrouping("cellBolt", new Fields("cell_num"));


		Config conf = new Config();

		conf.setDebug(false);
//		conf.setNumWorkers(2);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
                e.printStackTrace();
            }
        } else {
			System.err.println("Local running");
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}

	}

}
