package com.sxt.hadoop.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text mkey = new Text();
	IntWritable mval = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String[] strs = StringUtils.split(value.toString(), ' ');

		for (int i = 0; i < strs.length; i++) {
			mkey.set(getfof(strs[0], strs[i]));

			mval.set(0);
			context.write(mkey, mval);

			for (int j = i + 1; j < strs.length; j++) {

				mkey.set(getfof(strs[i], strs[j]));
				mval.set(1);
				context.write(mkey, mval);
			}
		}

	}

	public static String getfof(String s1, String s2) {
		if (s1.compareTo(s2) > 0) {
			return s1 + ":" + s2;

		}
		return s2 + ":" + s1;
	}

}
