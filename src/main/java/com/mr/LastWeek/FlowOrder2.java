package com.mr.LastWeek;

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
 * Created by Administrator on 2018/01/18.
 */
public class FlowOrder2 {
    public static class ForMapper extends Mapper<LongWritable,Text,FlowOrder,NullWritable>{
        FlowOrder ivalue = new FlowOrder();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            ivalue.setUpFlow(Integer.parseInt(str[7]));
            ivalue.setDownFlow(Integer.parseInt(str[8]));
            ivalue.setAllFlow(Integer.parseInt(str[9]));
            ivalue.setPhoneNumber(str[1]);
            context.write(ivalue,NullWritable.get());
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(FlowOrder.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(FlowOrder.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("G:\\aa\\phone1.dat"));
        FileOutputFormat.setOutputPath(job,new Path("G:\\output"));
        job.waitForCompletion(true);
    }
}
