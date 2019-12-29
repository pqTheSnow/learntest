package com.shsxt.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author qiong.peng
 * @Date 2019/10/17
 */
public class WordCountSpout extends BaseRichSpout {

    private Random random = new Random();

    private String[] array = {
            "Fear can hold you prisoner",
            "hope can set you free",
            "A strong man can save himself",
            "a great man can save another"
    };

    private SpoutOutputCollector collector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(array[random.nextInt(array.length)]));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
