package com.mr.second;

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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2018/01/23.
 */
/*public class ValueKey {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text ikey = new Text();
        private Text ivalue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs [] = value.toString().split(":");
            String own = strs[0];
            String[] friends = strs[1].split(",");
            for (String s:friends){
                ikey.set(s);
                ivalue.set(own);
//                context.write(ikey,ivalue);
            }
        }
    }
    public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        private Text ikey = new Text();
        private Text ivalue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ikey.set(key.toString()+"----");
            StringBuilder sb = new StringBuilder();
            for (Text text:values){
                sb.append(text.toString()+"、");
            }
            ivalue.set(sb.toString());
            context.write(ikey,ivalue);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path path = new Path("G:\\hadoopout");
        FileInputFormat.setInputPaths(job,new Path("G:\\hadoopin"));
        FileSystem fs = FileSystem.get(new URI("file://G://hadoopin"),new Configuration());
        if (fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,new Path("G:\\hadoopout"));
        job.waitForCompletion(true);
    }

}*/
