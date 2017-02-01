package com.myhadoop.mr.weblogpreprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * created by 徐宁 on 2017/2/1.
 */
public class WeblogPreProcess {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJar("D:/Idea/bigdata/out/artifacts/hadoopJar.jar");

        job.setMapperClass(WeblogPreProcessMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("C:\\hadoop\\webLogPreprocess\\in"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop\\webLogPreprocess\\out"));

        job.waitForCompletion(true);

    }

    static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            //可以插入一个静态资源过滤（.....）
            /*WebLogParser.filterStaticResource(webLogBean);*/
            if (!webLogBean.isValid()) {
                return;
            }
            k.set(webLogBean.toString());
            context.write(k, v);
        }

    }
}
