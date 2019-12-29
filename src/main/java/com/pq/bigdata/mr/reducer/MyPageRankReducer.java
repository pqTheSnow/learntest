package com.pq.bigdata.mr.reducer;

import com.pq.bigdata.mr.entity.Node;
import com.pq.bigdata.mr.job.MyPageRankJob;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/9/18
 */
public class MyPageRankReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 页面经过一轮PageRank算法获得的pr值，即values中从其他节点获得的pr值得和，需要排除自己的原始数据
        double sum = 0;
        Node currentNode = null;
        for (Text value : values) {
            Node node = Node.fromMR(value.toString());
            if (!node.containsAdjacentNodes()) {
                sum += node.getPageRank();
            } else {
                currentNode = node;
            }
        }
        // 按照公式计算新的pr值
        double newPr = (0.15 / 4.0) + (0.85 * sum);

        // 计算新的pr值和旧的pr值得差(即计算偏移量)
        double d = newPr - currentNode.getPageRank();
        // 保留3位有效数字(差值扩大一千倍)
        int j = Math.abs((int) (d * 1000));
        // 累计的和就是累计的偏移量
        context.getCounter(MyPageRankJob.MyCounter.MY).increment(j);

        currentNode.setPageRank(newPr);
        // 将当前页面的最新pr值和出链页面写出
        context.write(key, new Text(currentNode.toString()));
    }


}
