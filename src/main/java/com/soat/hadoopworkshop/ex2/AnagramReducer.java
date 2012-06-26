package com.soat.hadoopworkshop.ex2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnagramReducer extends
		Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer text = new StringBuffer();
        for (Text value : values) {
            text.append(value.toString());
            text.append(", ");
        }
        context.write(new Text(text.toString()), new IntWritable(1));
    }
}
