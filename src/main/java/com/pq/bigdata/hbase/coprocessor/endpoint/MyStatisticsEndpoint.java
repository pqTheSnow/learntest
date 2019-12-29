package com.pq.bigdata.hbase.coprocessor.endpoint;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * TODO 该文件根据rowCount.proto生成
 * 命令：protoc --java_out=./src rowCount.proto
 *
 * @author qiong.peng
 * @Date 2019/12/15
 */
public class MyStatisticsEndpoint extends MyStatisticsInterface.myStatisticsService implements Coprocessor,
        CoprocessorService {

    // 就是rowCount.proto中type对应的值
    private static final String STATISTICS_COUNT = "COUNT";

    // 就是rowCount.proto中type对应的值
    private static final String STATISTICS_SUM = "SUM";

    private RegionCoprocessorEnvironment envi;

    @Override
    public void getStatisticsResult(RpcController controller, MyStatisticsInterface.getStatisticsRequest request, RpcCallback<MyStatisticsInterface.getStatisticsResponse> done) {
        int result = 0;

        // 定义返回的Response
        MyStatisticsInterface.getStatisticsResponse.Builder responseBuilder = MyStatisticsInterface.getStatisticsResponse.newBuilder();

        // type就是在proto中定义参数字段，如果有多个参数字段可以都可以使用request.getXxx()来获取
        String type = request.getType();
        String gamilyName = new Random().nextInt(99) + request.getFamilyName();

        // 无类型，则返回0
        if (StringUtils.isEmpty(type)) {
            responseBuilder.setResult(0);
            done.run(responseBuilder.build());
            return;
        }

        if (STATISTICS_COUNT.equals(type)) {
            // 计算hbase表的行数
            result = count(result);
        } else if (STATISTICS_SUM.equals(type)) {
            // 计算hbase表某一列的值的和
            result = sum(result, request);
        } else {
            System.err.println("the type is not match!");
        }

        responseBuilder.setResult(result);
        done.run(responseBuilder.build());
    }

    private int count(int result) {
        InternalScanner scanner = null;
        try {
            Scan scan = new Scan();
            scanner = this.envi.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<Cell>();
            boolean hasMore = false;

            do {
                hasMore = scanner.next(results);
                result++;
            } while (hasMore);
        } catch (IOException ioe) {
            System.err.println("error happend when count in "
                    + this.envi.getRegion().getRegionNameAsString()
                    + " error is " + ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                    // nothing to do
                }
            }
        }
        return result;
    }

    private int sum(int result, MyStatisticsInterface.getStatisticsRequest request) {
        // 此时需要去检查客户端是否指定了列族和列名
        String famillyName = request.getFamilyName();
        String columnName = request.getColumnName();

        // 此条件下列族和列名是必须的
        if (!StringUtils.isBlank(famillyName)
                && !StringUtils.isBlank(columnName)) {

            InternalScanner scanner = null;
            try {
                Scan scan = new Scan();
                scan.addColumn(Bytes.toBytes(famillyName), Bytes.toBytes(columnName));
                scanner = this.envi.getRegion().getScanner(scan);
                List<Cell> results = new ArrayList<Cell>();
                boolean hasMore = false;
                do {
                    hasMore = scanner.next(results);
                    if (results.size() == 1) {
                        // 按行读取数据，并进行加和操作
                        result = result + Integer.valueOf(Bytes.toString(CellUtil.cloneValue(results.get(0))));
                    }

                    results.clear();
                } while (hasMore);

            } catch (Exception e) {
                System.err.println("error happend when count in "
                        + this.envi.getRegion().getRegionNameAsString()
                        + " error is " + e);
            } finally {
                if (scanner != null) {
                    try {
                        scanner.close();
                    } catch (IOException ignored) {
                        // nothing to do
                    }
                }
            }
        }
        return result;
    }

    // 协处理器是运行于region中的，每一个region都会加载协处理器
    // 这个方法会在regionserver打开region时候执行（还没有真正打开）
    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        // 需要检查当前环境是否在region上
        if (env instanceof RegionCoprocessorEnvironment) {
            this.envi = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    // 这个方法会在regionserver关闭region时候执行（还没有真正关闭）
    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do
    }

    /**
     * // rpc服务，返回本身即可，因为此类实例就是一个服务实现
     *
     * @return
     */
    @Override
    public Service getService() {
        return this;
    }
}
