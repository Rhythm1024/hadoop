package com.mr.KPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 2018/01/24.
 */
public class Ip {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        Text ikey = new Text();
        Text ivalue = new Text();
        KPIBean kpi;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            kpi = KPIBean.parse(value.toString());
            if (kpi.getRequestStatus().equals("200")) {
                ikey.set(kpi.getRequestPage()+"--"+kpi.getUserIp());
                ivalue.set(kpi.getUserIp());
            }
            context.write(ikey,ivalue);
        }
    }

    public static class ForReducer extends Reducer<Text, Text, Text, IntWritable> {
        IntWritable ivalue = new IntWritable();
        Text ikey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> ips = new HashSet<String>();
            for (Text ip : values) {
                ips.add(ip.toString());
                ikey.set(key);
            }
            ivalue.set(ips.size());
            context.write(ikey, ivalue);
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        Path path = new Path("G:\\hadoopout\\IP");
        FileInputFormat.setInputPaths(job,new Path("G:\\hadoopin"));
        FileSystem fs = FileSystem.get(new URI("file://G://hadoopin"),new Configuration());
        if (fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path("G:\\hadoopout\\IP"));
        job.waitForCompletion(true);
    }
}