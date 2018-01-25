package com.mr.LastWeek;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2018/01/22.
 */
public class InverseIndex {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text okey = new Text();
        private  Text ovalue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String[] strs = value.toString().split(" ");
            for (String s:strs){
                okey.set(s+" "+filename);
                context.write(okey,ovalue);
            }
        }
    }
    public static class ForCombiner extends Reducer<Text,Text,Text,Text>{
        private Text okey = new Text();
        private Text ovalue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text text:values){
                count++;
            }
            String[] strs = key.toString().split(" ");
            String forkey = strs[0];
            String forvalue = strs[1]+"-->"+count;
            okey.set(forkey);
            ovalue.set(forvalue);
            context.write(okey,ovalue);

        }
    }
    public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        private Text okey = new Text();
        private Text ovalue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text text:values){
                sb.append(text.toString()+"\t");
            }
            okey.set(key.toString()+":");
            ovalue.set(values.toString());
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) {
       // JobUtil.commitJob(InverseIndex.class,"","");
    }
}
