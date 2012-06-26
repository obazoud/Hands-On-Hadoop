package com.soat.hadoopworkshop.ex2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sun.reflect.generics.tree.ArrayTypeSignature;

public class AnagramMapper extends
		Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        char[] text = value.toString().toCharArray();
        Arrays.sort(text);
        context.write(new Text(new String(text)), value);
    }
}
