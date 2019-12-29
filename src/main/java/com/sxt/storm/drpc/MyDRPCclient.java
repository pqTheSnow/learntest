package com.sxt.storm.drpc;//package com.sxt.storm.drpc;


import backtype.storm.Config;
import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.transport.TTransportException;

public class MyDRPCclient {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Config config = new Config();

        DRPCClient client = new DRPCClient(config, "node02", 3772);

        String result = client.execute("reach", "remote/fjsdlf/1");

        System.out.println(result);

    }
}
