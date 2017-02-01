package com.myhadoop.mr.inverseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by 徐宁 on 2017/1/23.
 */
public class InverseIndexStepOne {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJar("D:/Idea/bigdata/out/artifacts/hadoopJar.jar");
//        job.setJarByClass(InverIndexStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\hadoop\\InverseIndex\\in"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop\\InverseIndex\\out"));
        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(InverseIndexStepOneMapper.class);
        job.setReducerClass(InverseIndexStepOneReducer.class);

        job.waitForCompletion(true);

    }

    private static class InverseIndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();
        IntWritable v = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            String[] words = line.split(" ");
            // 获取切片
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            // 获取文件名
            String fileName = inputSplit.getPath().getName();
            for (String word : words) {
                k.set(word + "--" + fileName);
                context.write(k, v);
            }

        }
    }

    private static class InverseIndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable intWritable = new IntWritable(0);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            intWritable.set(count);
            context.write(key, intWritable);
        }
    }
}
