package com.pq.test;

import com.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;

/**
 * @author qiong.peng
 * @Date 2019/9/21
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HBaseDemo {

    private Configuration conf;
    private HBaseAdmin hBaseAdmin;

    //声明表的名字
    private final static String SXT_TABLE_NAME = "school";
    private HTable hTable;

    @Test
    public void scan() throws IOException {
        Scan scan = new Scan();
        // [a, Z)尽然扫不到‘sc01’
        scan.setStartRow("sc00".getBytes());
        scan.setStopRow("sc02".getBytes());
        // 开始扫描
        ResultScanner scanner = hTable.getScanner(scan);
        // 开始迭代
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("---------------------------------");
        }
        // 关闭扫描器
        scanner.close();
    }

    @Test
    public void get() throws IOException {
        Get get = new Get("sc01".getBytes());
        // get.addColumn()是筛选条件
        get.addColumn("info".getBytes(), "name".getBytes());
        get.addColumn("info".getBytes(), "createTime".getBytes());
        get.addColumn("info".getBytes(), "studentNum".getBytes());
        Result r = hTable.get(get);
        Cell nameCell = r.getColumnLatestCell("info".getBytes(), "name".getBytes());
        Cell studentNumCell = r.getColumnLatestCell("info".getBytes(), "studentNum".getBytes());
        Cell createTimeCell = r.getColumnLatestCell("info".getBytes(), "createTime".getBytes());
        System.out.println(new String(CellUtil.cloneValue(nameCell)));
        System.out.println(new String(CellUtil.cloneValue(studentNumCell)));
        System.out.println(new String(CellUtil.cloneValue(createTimeCell)));
    }

    @Test
    public void put() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put("sc01".getBytes());
        // 设置列族对应列的数据
        put.add("info".getBytes(), "name".getBytes(), "实验".getBytes());
        put.add("info".getBytes(), "studentNum".getBytes(), "666".getBytes());
        put.add("info".getBytes(), "createTime".getBytes(), String.valueOf(System.currentTimeMillis()).getBytes());
        // 插入数据
        hTable.put(put);
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        // 获取表
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(SXT_TABLE_NAME));
        // 创建列族
        HColumnDescriptor infoColumn = new HColumnDescriptor("info");
        infoColumn.setMaxVersions(3);
        HColumnDescriptor historyColumn = new HColumnDescriptor("history");
        // 将列族添加到表对象
        hTableDescriptor.addFamily(infoColumn);
        hTableDescriptor.addFamily(historyColumn);
        // 创建表对象
        hBaseAdmin.createTable(hTableDescriptor);
    }

    @Before
    public void init() throws IOException {
        // 获取噢诶之文件
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "basenode201:2181,basenode202:2181,basenode203:2181");
        // 创建Hbase连接
        hBaseAdmin = new HBaseAdmin(conf);
        // 获取到表的连接
        hTable = new HTable(conf, SXT_TABLE_NAME);
    }

    @After
    public void destroy() throws IOException {
        if (hTable != null) {
            hTable.close();
        }
        if (hBaseAdmin != null) {
            hBaseAdmin.close();
        }
    }

}
