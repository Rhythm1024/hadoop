package com.mr.LastWeek;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.file.FileSystem;

/**
 * Created by Administrator on 2018/01/18.
 */
public class MROrder {
    public static class ForMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int i =Integer.parseInt(line);
            System.out.println(i);
            IntWritable ikey = new IntWritable(i);
            context.write(ikey,NullWritable.get());
        }
    }
    public static class ForReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("text   ------  "+key);
            context.write(key,NullWritable.get());
        }
    }
    public static class MyComparator extends IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setSortComparatorClass(MyComparator.class);
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\aa\\a.txt"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\output"));
        job.waitForCompletion(true);
    }
}
