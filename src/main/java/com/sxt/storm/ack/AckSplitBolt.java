package com.sxt.storm.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class AckSplitBolt extends BaseRichBolt {

    OutputCollector collector ;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String[] split = line.split(" ");

        for (int i = 0; i < split.length-1; i++) {
            collector.emit(new Values(split[i]));
            // 继续往下发送锚点，继续跟踪消息
//            collector.emit(input, new Values(split[i]));
        }

        collector.ack(input);
//        collector.fail(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
