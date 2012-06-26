package com.soat.hadoopworkshop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostFrequentReducer extends
		Reducer<Text, Text, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

        String maxText = "";
        int maxCount = 0;
        for (Text value : values) {
            String[] split = value.toString().split("\t");
            System.out.println(value.toString() + ": " + split.length);
            if (split.length == 2) {
                String txt = split[0];
                int count = Integer.parseInt(split[1]);
                if (count > maxCount) {
                    maxText = txt;
                    maxCount = count;
                }
            }
        }
        context.write(new Text(maxText), new IntWritable(maxCount));
	}

}
