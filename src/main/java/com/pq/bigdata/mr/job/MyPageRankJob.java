package com.pq.bigdata.mr.job;

import com.pq.bigdata.mr.mapper.MyPageRankMapper;
import com.pq.bigdata.mr.reducer.MyPageRankReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/18
 */
public class MyPageRankJob {

    public static enum MyCounter{
        MY
    }

    public static void main(String[] args) throws Exception {

        // 迭代计数器
        int i = 0;
        Configuration conf = new Configuration(true);
        //收敛的指标
        double d = 0.0001;
        // 不能设置页面数，应为在分布式的系统中，无法总有效的计数
//        conf.setInt("pageCount", 0);
        while (true) {
            i++;
            try {
                // 标志任务是第几次执行，用于区别第一次执行
                conf.setInt("runCount", i);
                // 创建任务
                Job job = Job.getInstance(conf);
                //设置jar作业的主类
                job.setJarByClass(MyPageRankJob.class);
                // 设置作业名称
                job.setJobName("MyPageRankJob");
                // 设置reduceTask的个数
                job.setNumReduceTasks(2);

                FileSystem fs = FileSystem.get(conf);

                //设置读取数据的路径(第一次)
                Path inputPath = new Path("/user/root/pr/input/");
                if (i != 1) {
                    // 设置要处理文件的路径
                    inputPath = new Path("/user/root/pr/output/pr" + (i - 1));
                }
                // 这里读取数据文件目录
                FileInputFormat.addInputPath(job, inputPath);

                // 设置结果的输出目录
                Path outputPath = new Path("/user/root/pr/output/pr" + i);
                // 如果已经存在则删除，方便重复测试
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                FileOutputFormat.setOutputPath(job, outputPath);

                // 定义Map的输出格式
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // 设置Mapper类
                job.setMapperClass(MyPageRankMapper.class);
                // 设置Reducer类
                job.setReducerClass(MyPageRankReducer.class);

                // 设置输入到map中的数据格式，默认是TextInputFormat.class
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                // 提交作业
                boolean flag = job.waitForCompletion(true);
                if (flag) {
                    System.out.println("--------------------------success." + i);
                    // 每次新的job，sum都是新的
                    long sum = job.getCounters().findCounter(MyCounter.MY).getValue();
                    System.out.println("--------------------------sum=" + sum);
                    // 计算的时候注意数据类型int/int得到的是整数 TODO 建议使用BigDecimal
                    double avgd = sum / 4000.0;
                    if (avgd < d) {
                        break;
                    }
                }
            } catch (Exception e) {
                // 出错也不退出循环
                e.printStackTrace();
            }
        }
    }
}
