package com.sxt.hadoop.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TCombiner extends Reducer<TQ, IntWritable, TQ, IntWritable> {

	protected void reduce(TQ key, Iterable<IntWritable> value,
			Reducer<TQ, IntWritable, TQ, IntWritable>.Context context) throws IOException, InterruptedException {

		// 相同的key为一组调用一个reduce方法

		int flag = 0;
		int day = 0;
		for (IntWritable val : value) {

			if (flag == 0) {
				context.write(key, val);
				flag++;
				day = key.getDay();
			}
			if (flag != 0 && day != key.getDay()) {
				context.write(key, val);
				break;
			}

		}

	}
}
