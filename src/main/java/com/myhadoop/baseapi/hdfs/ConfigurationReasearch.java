package com.myhadoop.baseapi.hdfs;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationReasearch {
	
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		conf.addResource("test.xml");
		System.out.println(conf.get("xxx.uu"));
		
//		Iterator<Entry<String, String>> it = conf.iterator();
//		
//		while(it.hasNext()){
//			
//			System.out.println(it.next());
//			
//		}
		
		
		
	}
	

}
