package com.soat.hadoopworkshop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostFrequentMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

        context.write(new Text("key"), value);
	}

}
