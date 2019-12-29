package com.pq.bigdata.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @author qiong.peng
 * @Date 2019/12/16
 */
public class ESDemo01 {

    private static String elasticIp = "192.169.0.23";
    private static int elasticPort = 9200;

    public static RestHighLevelClient client = null;
    /*
     * 初始化服务
     */
    private static void init() {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticIp, elasticPort));
        client = new RestHighLevelClient(restClientBuilder);
    }

    public static void main(String[] args) {
        //
    }

    public static void add1() throws IOException {
        String index = "index1";
        String type = "type";
        String id = "id_1";

        IndexRequest request = new IndexRequest(index, type, id);
        String json = "{" + "\"uid\":\"1234\","+ "\"phone\":\"12345678909\","+ "\"msgcode\":\"1\"," + "\"sendtime\":\"2019-03-14 01:57:04\","
                + "\"message\":\"xuwujing study Elasticsearch\"" + "}";
        request.source(json);
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        boolean isCreated = response.isCreated();
        if(isCreated) {
            System.out.println("索引创建成功");
        } else {
            System.out.println("索引创建失败");
        }
    }
}
