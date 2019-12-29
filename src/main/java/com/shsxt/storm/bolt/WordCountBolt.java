package com.shsxt.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author qiong.peng
 * @Date 2019/10/17
 */
public class WordCountBolt extends BaseRichBolt {

    Map<String, Integer> countMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = countMap.get(word);
        if(count == null) {
            count = 1;
        } else {
            count = count + 1;
        }
        countMap.put(word, count);
        System.out.println(word + " count is " + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
