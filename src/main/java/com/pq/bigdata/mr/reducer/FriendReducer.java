package com.pq.bigdata.mr.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/16
 */
public class FriendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean flag = true;
        int count = 0;
        for (IntWritable value : values) {
            int tmp = value.get();
            count += tmp;
            if(tmp == 0) {
                flag = false;
            }
        }
        if (flag) {
            context.write(key, new IntWritable(count));
        }
    }
}
