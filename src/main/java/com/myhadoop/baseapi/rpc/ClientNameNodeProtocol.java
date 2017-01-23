package com.myhadoop.baseapi.rpc;


/**
 * hdfs客户端跟namenode之间进行远程过程调用使用的协议——接口
 * @author
 *
 */
public interface ClientNameNodeProtocol {

	static final long versionID = 100L;

	String getMetaData(String path);

}
