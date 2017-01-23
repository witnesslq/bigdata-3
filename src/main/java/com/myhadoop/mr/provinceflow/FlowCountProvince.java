package com.myhadoop.mr.provinceflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 徐宁 on 2017/1/23.
 */
public class FlowCountProvince {

    private static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBeanProvince> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();    //将一行内容转成string
            String[] fields = line.split("\t");    //切分字段
            String phoneNbr = fields[1];    //取出手机号

            long upFlow = Long.parseLong(fields[fields.length - 3]);    //取出上行流量下行流量
            long dFlow = Long.parseLong(fields[fields.length - 2]);

            context.write(new Text(phoneNbr), new FlowBeanProvince(upFlow, dFlow));
        }
    }

    private static class FlowCountReducer extends Reducer<Text, FlowBeanProvince, Text, FlowBeanProvince> {
        //<183323,bean1><183323,bean2><183323,bean3><183323,bean4>.......
        @Override
        protected void reduce(Text key, Iterable<FlowBeanProvince> values, Context context) throws IOException, InterruptedException {

            long sum_upFlow = 0;
            long sum_dFlow = 0;

            //遍历所有bean，将其中的上行流量，下行流量分别累加
            for (FlowBeanProvince bean : values) {
                sum_upFlow += bean.getUpFlow();
                sum_dFlow += bean.getdFlow();
            }

            FlowBeanProvince resultBean = new FlowBeanProvince(sum_upFlow, sum_dFlow);
            context.write(key, resultBean);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = "file:/C:/hadoop/proviceflow/in";
            args[1] = "file:/C:/hadoop/proviceflow/out";
        }

        Configuration conf = new Configuration();
        /*conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resoucemanager.hostname", "mini1");*/
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJar("D:/Idea/bigdata/out/artifacts/hadoopJar.jar");
        //指定本程序的jar包所在的本地路径
        //job.setJarByClass(FlowCount.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setPartitionerClass(PartitionerProvince.class);
        job.setNumReduceTasks(5);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBeanProvince.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanProvince.class);

        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, outPath);

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
