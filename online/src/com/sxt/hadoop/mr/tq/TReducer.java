package com.sxt.hadoop.mr.tq;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TReducer extends Reducer<TQ, IntWritable, Text, IntWritable> {
	Text rkey = new Text();
	IntWritable rval = new IntWritable();

	@Override
	protected void reduce(TQ key, Iterable<IntWritable> value, Reducer<TQ, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		// 相同的key为一组调用一个reduce方法

		int flag = 0;
		int day = 0;
		for (@SuppressWarnings("unused") IntWritable val : value) {

			if (flag == 0) {
				rval.set(key.getWd());
				rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				context.write(rkey, rval);
				flag++;
				day = key.getDay();
			}
			if (flag != 0 && day != key.getDay()) {
				rval.set(key.getWd());
				rkey.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay());
				context.write(rkey, rval);
				break;
			}

		}

	}
}
