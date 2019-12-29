package com.sxt.storm.test_ack;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AckTest {

    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("ack",new AckSpout());
        topologyBuilder.setBolt("splitbolt",new AckSplitBolt(),2).shuffleGrouping("ack");
        topologyBuilder.setBolt("countbolt",new AckCountBolt(),2).fieldsGrouping("splitbolt",new Fields("word"));
        Config conf = new Config();
        //超时时间
        conf.setMessageTimeoutSecs(3);
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("acktest",conf,topologyBuilder.createTopology());

    }
}
