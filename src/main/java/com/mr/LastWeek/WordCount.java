package com.mr.LastWeek;

import com.mr.util.Oput;
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
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ivalue = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            Pattern  pattern=Pattern.compile("\\w+");
            Matcher matcher=pattern.matcher(line);
            while(matcher.find()){
                String word=matcher.group();
                okey.set(word);
                context.write(okey,ivalue);
            }
        }
    }
    public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable nullWritable:values){
                sum++;
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        String path="G://output";
        FileInputFormat.setInputPaths(job,new Path("G:\\aa"));
        FileOutputFormat.setOutputPath(job,new Path(path));
        Oput.clear(path);
        job.waitForCompletion(true);
    }

}
