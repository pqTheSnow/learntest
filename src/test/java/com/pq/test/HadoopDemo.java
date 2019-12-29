package com.pq.test;

import com.Application;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;

/**
 * @author qiong.peng
 * @Date 2019/9/11
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class HadoopDemo {

    private Configuration conf;
    private FileSystem fs;

    @Before
    public void init() throws IOException {
        System.out.println("init");
        conf = new Configuration(true);
        fs = FileSystem.get(conf);
    }

    @After
    public void destroy() throws IOException {
        System.out.println("destroy");
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    public void path() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
        while (files.hasNext()) {
            LocatedFileStatus status = files.next();
            System.out.println(status.getPath());
        }
//        fs.open(path);
    }

    @Test
    public void upload() throws Exception {
        Path path = new Path("/user/root/JOBS.txt");
        InputStream is = new FileInputStream("D:\\tmp\\JOBS.txt");
        FSDataOutputStream os = fs.create(path);

        IOUtils.copyBytes(is, os, conf);
    }

    @Test
    public void download() throws Exception {
        Path path = new Path("/");
        InputStream is = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileOutputStream fos = new FileOutputStream("xxx");
        IOUtils.copyBytes(is,fos,1024);
        is.close();
        fos.close();
    }

}
