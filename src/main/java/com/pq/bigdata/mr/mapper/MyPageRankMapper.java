package com.pq.bigdata.mr.mapper;

import com.pq.bigdata.mr.entity.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/18
 */
public class MyPageRankMapper extends Mapper<Text, Text, Text, Text> {

    // 页面的初始pr值
    private static final String DEFAULT_PR_VALUE = "1.0";

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        // 如果是第一次读取文件，那么需要给PR设置默认值1
        int runCount = context.getConfiguration().getInt("runCount", 1);
        // 自定义封装对象，包括页面的pr值和出链页面
        Node node = null;
        if (runCount == 1) {
            // 页面的初始pr值为1
            node = Node.fromMR(DEFAULT_PR_VALUE, value.toString());
        } else {
            node = Node.fromMR(value.toString());
        }
        // 将当前页面的和其对应的pr值和出链页面写出，用于输出使用
        context.write(key, new Text(node.toString()));

        // 计算当前页面的出链页面获得该页面的pr值
        if (node.containsAdjacentNodes()) {
            // TODO 此处使用BigDecimal是不是好一些，可以控制计算的精度
            // 计算出链获得的平均pr值
            double d = node.getPageRank() / node.getAdjacentNodeNames().length;
            for (String p : node.getAdjacentNodeNames()) {
                context.write(new Text(p), new Text(String.valueOf(d)));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String value = "A\tB\tD";
        Node node = null;
        if (true) {
            // 页面的初始pr值为1
            node = Node.fromMR(DEFAULT_PR_VALUE, value.toString());
        } else {
            node = Node.fromMR(value.toString());
        }
        System.out.println("args = [" + node + "]");
    }
}
