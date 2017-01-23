package com.myhadoop.baseapi.rpc.controller;

import com.myhadoop.baseapi.rpc.ClientNameNodeProtocol;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HdfsClient {

    public static void main(String[] args) throws IOException {

        ClientNameNodeProtocol namenode = RPC.getProxy(ClientNameNodeProtocol.class, 100L, new InetSocketAddress("hdp-node01", 1413), new Configuration());

        String metaData = namenode.getMetaData("/苍老师初解禁.avi");

        System.out.println(metaData);


    }


}
