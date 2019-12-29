package com.pq.bigdata.mr.job;

import java.io.IOException;

import com.pq.bigdata.mr.entity.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankJob {

	public static enum Mycounter {
		my
	}

	public static void main(String[] args) {
		//获取配置文件
		Configuration conf = new Configuration(true);
		//跨平台执行
//		conf.set("mapreduce.app-submission.corss-paltform", "true");
		//如果分布式运行,必须打jar包 且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
//		conf.set("mapreduce.framework.name", "local");
		//这个配置,只属于,切换分布式到本地单进程模拟运行的配置
		//这种方式不是分布式,所以不用打jar包

		//收敛的指标
		double d = 0.0001;

		//计算迭代收敛的次数
		int i = 0;
		//开始执行迭代收敛
		while (true) {
			i++;
			try {
				//向配置文件中设置一个变量
				conf.setInt("runCount", i);
				//获取分布式文件系统
				FileSystem fs = FileSystem.get(conf);
				//创建JOB,并设置基本信息
				Job job = Job.getInstance(conf);
				job.setJarByClass(PageRankJob.class);
				job.setJobName("pagerank" + i);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				//使用了新的输入格式化类
				job.setInputFormatClass(KeyValueTextInputFormat.class);

				//设置读取数据的路径(第一次)
				Path inputPath = new Path("/shsxt/pagerank/input/");

				//第二次之后读取数据,就是前一次的结果
				if (i > 1) {
					inputPath = new Path("/shsxt/pagerank/output/pr" + (i - 1));
				}
				//读取数据的路径
				FileInputFormat.addInputPath(job, inputPath);
				//本次输出路径
				Path outpath = new Path("/shsxt/pagerank/output/pr" + i);
				if (fs.exists(outpath)) {
					fs.delete(outpath, true);
				}
				//设置输出路径
				FileOutputFormat.setOutputPath(job, outpath);
				//提交任务,并获取本次任务是否成功
				boolean flag = job.waitForCompletion(true);
				if (flag) {
					System.out.println("--------------------------success." + i);
					// TODO 每次循环都是一个新的job，故Counter中的值都是属于自己的job，不同的job的计数器不会叠加
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();
					System.out.println("+++++++++++++++++++++++++sum." + sum);
					System.out.println(sum);
					double avgd = sum / 4000.0;
					//如果平均值达到收敛指标，停止循环
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @author Administrator
	 *
	 */
	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {

		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			//如果是第一次读取文件，那么需要给PR设置默认值1
			int runCount = context.getConfiguration().getInt("runCount", 1);

			//以空格切分当前行，第一个空格前为KEY，其余数据为Value
			String page = key.toString();
			//定义对象Node
			Node node = null;
			//判断是否为第一次加载,将数据封装成一个对象{pr,子连接}
			if (runCount == 1) {
				//A	B D
				//K:A
				//V:B D
				node = Node.fromMR("1.0", value.toString());
			} else {
				//A 0.3 B D
				//K:A
				//V:0.3	B D
				node = Node.fromMR(value.toString());
			}
			//传递老的pr值和对应的页面关系 A:1.0	B	D
			context.write(new Text(page), new Text(node.toString()));

			//开始计算每个节点本次应得的pr值
			if (node.containsAdjacentNodes()) {
				//每个节点的pr=当前页面pr/出链的数量
				double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
				//开始写出子节点和pr值
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					//页面A投给谁，谁作为key，val是票面值，票面值为：每个节点的pr
					context.write(new Text(outPage), new Text(outValue + ""));
				}
			}
		}
	}

	/**
	 * 
	 * @author Administrator
	 *
	 */
	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
			//相同的key为一组

			//页面对应关系及老的pr值
			//B:1.0 C
			//新的投票值
			//B:0.5		
			//B:0.5
			//B:0.5
			//最终写出的结果
			//B 1.5 C

			//定义变量存放新的PR值
			double sum = 0.0;

			Node sourceNode = null;
			for (Text i : iterable) {
				//创建新的节点
				Node node = Node.fromMR(i.toString());
				//判断是老的映射关系还是新的PR值
				if (node.containsAdjacentNodes()) {
					sourceNode = node;
				} else {
					sum = sum + node.getPageRank();
				}
			}

			// 4为页面总数
			double newPR = (0.15 / 4.0) + (0.85 * sum);
			System.out.println("*********** new pageRank value is " + newPR);

			//把新的pr值和计算之前的pr比较
			double d = newPR - sourceNode.getPageRank();
			//保留四位有效数字,然后取绝对值
			int j = Math.abs((int) (d * 1000.0));
			context.getCounter(Mycounter.my).increment(j);

			//将当前网站的PR值写出
			sourceNode.setPageRank(newPR);
			context.write(key, new Text(sourceNode.toString()));
		}
	}
}
