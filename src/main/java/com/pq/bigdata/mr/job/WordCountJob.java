package com.pq.bigdata.mr.job;

import com.pq.bigdata.mr.mapper.WordCountMapper;
import com.pq.bigdata.mr.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author qiong.peng
 * @Date 2019/9/12
 */
public class WordCountJob {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        // 创建任务
        Job job = Job.getInstance(conf);
        //设置jar作业的主类
        job.setJarByClass(WordCountJob.class);
        // 设置作业名称
        job.setJobName("WordCount");
        // 设置reduceTask的个数
        job.setNumReduceTasks(3);

        // 设置要处理文件的路径
        FileInputFormat.setInputPaths(job, new Path("/user/root/JOBS.txt"));

        // 设置结果的输出目录
        FileOutputFormat.setOutputPath(job, new Path("/user/root/result" + System.currentTimeMillis()));

        // 定义Map的输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Mapper类
        job.setMapperClass(WordCountMapper.class);
        // 设置Reducer类
        job.setReducerClass(WordCountReducer.class);

        // 提交作业
        job.waitForCompletion(true);
    }
}
