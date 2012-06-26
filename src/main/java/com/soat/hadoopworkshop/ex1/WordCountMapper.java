package com.soat.hadoopworkshop.ex1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// Utiliser context.write(text, int) pour ecrire la sortie du mapper
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()) {
            context.write(new Text(stringTokenizer.nextToken()), new IntWritable(1));
        }
	}

}
