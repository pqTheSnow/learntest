package com.pq.bigdata.mr.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/16
 */
public class FriendMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable direct = new IntWritable(0);
    private IntWritable inDirect = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = value.toString().split(" ");
        // 直接好友
        for (int i = 1; i < array.length; i++) {
            context.write(new Text(sort(array[0], array[i])), direct);
        }

        // 间接好友
        for (int i = 1; i < array.length; i++) {
            for (int j = i + 1; j < array.length; j++) {
                context.write(new Text(sort(array[i], array[j])), inDirect);
            }
        }
    }

    private static String sort(String s1, String s2) {
        if(s1.compareTo(s2) > 0) {
            return s2 + ":" + s1;
        }
        return s1 + ":" + s2;
    }
}
