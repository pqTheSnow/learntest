package com.shsxt.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.shsxt.storm.bolt.SumBolt;
import com.shsxt.storm.bolt.WordCountBolt;
import com.shsxt.storm.bolt.WordCountSplitBolt;
import com.shsxt.storm.spout.SumSpout;
import com.shsxt.storm.spout.WordCountSpout;

/**
 * @author qiong.peng
 * @Date 2019/10/17
 */
public class StormDemo {
    public static void main(String[] args) {
//        sumStorm();
//        add();
        wordCountStorm();
    }


    public static void wordCountStorm() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("wcSpout", new WordCountSpout());
        // TODO 把xxGrouping()设置放在数据接收端而不放在数据发送端，
        // TODO 猜测是因为数据数据发送端修改的可能性小，故修改时，只需要修改Bolt就可以了，而不需要懂Spout，实际情况得看源码
        topologyBuilder.setBolt("wcSplit", new WordCountSplitBolt(), 2)
                .setNumTasks(4).shuffleGrouping("wcSpout");
        topologyBuilder.setBolt("wcCount", new WordCountBolt(), 5)
                .fieldsGrouping("wcSplit", new Fields("word"));

        StormTopology stormTopology = topologyBuilder.createTopology();

        Config config = new Config();
        // 设置进程数
        config.setNumWorkers(2);
        try {
            StormSubmitter.submitTopology("wc", config, stormTopology);
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("job-wc", config, topologyBuilder.createTopology());
    }

    public static void sumStorm() {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("sumSpout", new SumSpout());
        topologyBuilder.setBolt("sumBolt", new SumBolt()).shuffleGrouping("sumSpout");

        StormTopology stormTopology = topologyBuilder.createTopology();

        Config config = new Config();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("job-sum", config, stormTopology);
    }

    public static void add() {
        int sum = 0;
        for (int i = 0; i <= 22; i++) {
            sum += i;
        }
        System.out.println("sum = " + sum);
    }
}
