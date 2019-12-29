package com.sxt.storm.test_ack;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class AckSpout implements IRichSpout {


    SpoutOutputCollector collector;


    String[] lines = new String[]{
            "i love shsxt",
            "i hate you",
            "haha xixi xixi",
            "xidada is good"
    };

    Random random = new Random();

    int id = 0;
    //缓存...
    Map<Integer, String> map = new HashMap<>();


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void nextTuple() {
        int index = random.nextInt(lines.length);

        String line = lines[index];

        collector.emit(new Values(line), id);

        map.put(id, line);

        id++;

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 如果消息处理成功，则storm会调用此方法
     *
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        String line = map.remove(msgId);

        System.err.println("ack === mid: " + msgId + " value: " + line);
    }

    /**
     * 如果数据下游接收失败，则storm会调用此方法
     *
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {

        String line = map.get(msgId);

        System.err.println("fail ====  mid: " + msgId + " value: " + line);

        collector.emit(new Values(line), msgId);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void close() {

    }

}
