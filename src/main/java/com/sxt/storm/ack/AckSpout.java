package com.sxt.storm.ack;

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
    //缓存map...
    Map<Object,String> map =  new HashMap<>();

    String[] lines = new String[]{
            "i love shsxt",
            "i hate you",
            "haha xixi xixi",
            "xidada is good"
    };

    Random random = new Random();

    long id = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

    }


    @Override
    public void nextTuple() {

        String line = lines[random.nextInt(4)];
        collector.emit(new Values(line),id);
        map.put(id,line);

        id++;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *  如果数据被完整处理，此时调用ack方法，并把msgid穿进去
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        System.out.println(msgId + " 执行成功....并删除缓存");
        map.remove(msgId);
    }

    /**
     * 如果数据未被完整处理，即处理失败，则调用fail方法。
     * 失败的时候从缓存map里重发数据
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        System.out.println("执行失败，重发...  msgid: " + msgId);
        collector.emit(new Values(map.get(msgId)),msgId);
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
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }
}
