package com.sxt.storm.ack;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class AckCountBolt extends BaseRichBolt {

    Map<String,Integer> resultMap = new HashMap<>();

    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Integer integer = resultMap.get(word);
        if(integer==null){
            integer = 1;
        }else{
            integer++;
        }
        resultMap.put(word,integer);
        System.out.println(word + " : " + integer);
        //告诉ack 此tuple接受成功
        collector.ack(input);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
