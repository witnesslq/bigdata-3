package com.myhadoop.mr.inverseindex;

import org.apache.hadoop.conf.Configuration;
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
 * Created by 徐宁 on 2017/2/1.
 */
public class InverseIndexStepTwo {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJar("D:/Idea/bigdata/out/artifacts/hadoopJar.jar");
//        job.setJarByClass(InverIndexStepOne.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("C:\\hadoop\\InverseIndex\\in2"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\hadoop\\InverseIndex\\out2"));
        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(InverseIndexStepTwoMapper.class);
        job.setReducerClass(InverseIndexStepTwoReducer.class);

        job.waitForCompletion(true);

    }

    private static class InverseIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] files = line.split("--");
            context.write(new Text(files[0]), new Text(files[1]));
        }
    }

    private static class InverseIndexStepTwoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text text : values) {
                sb.append(text.toString().replace("\t", "-->") + "\t");
            }
            context.write(key, new Text(sb.toString()));
        }
    }
}
