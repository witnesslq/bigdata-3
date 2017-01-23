package com.myhadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by 徐宁 on 2017/1/23.
 * 相当于一个yarn集群的客户端
 * 需要在此封装我们的mr程序的相关运行参数，指定jar包
 * 最后提交给yarn
 *
 * @author
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        if (args == null || args.length == 0) {
            args = new String[2];
            args[0] = "file:/C:/hadoop/wordcount/wcin";
            args[1] = "file:/C:/hadoop/wordcount/wcout";
        }

        Configuration conf = new Configuration();
        /*conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resoucemanager.hostname", "mini1");*/
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        Job job = Job.getInstance(conf);

        job.setJar("D:/Idea/bigdata/out/artifacts/hadoopJar.jar");
        // 指定本程序的jar包所在的本地路径
        // job.setJarByClass(WordcountDriver.class);

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outPath);

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
