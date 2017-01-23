package com.myhadoop.mr.provinceflow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by 徐宁 on 2017/1/23.
 * K2  V2  对应的是map输出kv的类型
 */
public class PartitionerProvince extends Partitioner<Text, FlowBeanProvince> {
    private HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();

    {
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }


    @Override
    public int getPartition(Text key, FlowBeanProvince flowBean, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
