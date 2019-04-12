package com.sxt.hadoop.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable rval = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int flg = 0;
		int sum = 0;
		for (IntWritable v : values) {
			sum += v.get();
			if (v.get() == 0) {
				flg = 1;
			}
		}
		if (flg == 0) {
			rval.set(sum);
			context.write(key, rval);
		}

	}
}
