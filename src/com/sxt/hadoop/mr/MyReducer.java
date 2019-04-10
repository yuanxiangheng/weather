package com.sxt.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			IntWritable next = iterator.next();
			sum += next.get();
		}
		result.set(sum);
		context.write(key, result);
	}

}
