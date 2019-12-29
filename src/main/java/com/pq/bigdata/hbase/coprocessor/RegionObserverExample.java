package com.pq.bigdata.hbase.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * @author qiong.peng
 * @Date 2019/12/15
 */
public class RegionObserverExample extends BaseRegionObserver {
    public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        if(Bytes.equals(get.getRow(), FIXED_ROW)) {
            KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()));
            results.add(kv);
        }
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        if(Bytes.equals(get.getRow(), FIXED_ROW)) {
            KeyValue kv = new KeyValue(get.getRow(), FIXED_ROW, FIXED_ROW, Bytes.toBytes("随机数:" + new Random().nextInt(100)));
            results.add(kv);
        }
    }
}
