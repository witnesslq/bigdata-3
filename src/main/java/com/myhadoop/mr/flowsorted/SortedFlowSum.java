package com.myhadoop.mr.flowsorted;

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
public class SortedFlowSum {

    private static class SortedFlowSumMapper extends Mapper<LongWritable, Text, SortbleFlowBean, Text> {
        private SortbleFlowBean bean = new SortbleFlowBean();
        private Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] fields = line.split("\t");

            String phoneNbr = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long dFlow = Long.parseLong(fields[2]);

            bean.setdFlow(dFlow);
            bean.setUpFlow(upFlow);
            bean.setSumFlow(dFlow + upFlow);

            v.set(phoneNbr);

            context.write(bean, v);

        }
    }

    private static class SortedFlowSumReducer extends Reducer<SortbleFlowBean, Text, Text, SortbleFlowBean> {
        @Override
        protected void reduce(SortbleFlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), bean);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = "file:/C:/hadoop/flowsorted/in";
            args[1] = "file:/C:/hadoop/flowsorted/out";
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
        job.setMapperClass(SortedFlowSumMapper.class);
        job.setReducerClass(SortedFlowSumReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(SortbleFlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SortbleFlowBean.class);

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
