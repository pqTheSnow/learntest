package com.pq.bigdata.mr.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/12
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private IntWritable count = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] array = value.toString().split("\\W+");
        for (String str :
                array) {
            context.write(new Text(str), count);
        }
    }
}
