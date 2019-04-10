package com.sxt.hadoop.mr.weather;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TCombiner extends Reducer<TQ, IntWritable, TQ, IntWritable>{


	@Override
	protected void reduce(TQ key, Iterable<IntWritable> values, Reducer<TQ, IntWritable, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {

		int flg = 0;
		int day = 0;

		for (IntWritable val : values) {

			if (flg == 0) {
				context.write(key, val);
				flg++;
				day = key.getDay();

			}
			if (flg != 0 && day != key.getDay()) {
				context.write(key, val);
				break;
			}

		}

	}

}
