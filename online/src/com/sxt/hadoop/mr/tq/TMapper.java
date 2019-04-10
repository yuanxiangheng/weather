package com.sxt.hadoop.mr.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable> {

	TQ mkey = new TQ();
	IntWritable mval = new IntWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TQ, IntWritable>.Context context)
			throws IOException, InterruptedException {

		try {

			String[] strs = StringUtils.split(value.toString(), '\t');

			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("");
			Date date = simpleDateFormat.parse(strs[0]);

			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			mkey.setYear(cal.get(Calendar.YEAR));
			mkey.setMonth(cal.get(Calendar.MONTH));
			mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));

			String wdString = strs[1].substring(0, strs[1].length() - 1);
			int wd = Integer.parseInt(wdString);
			mkey.setWd(wd);
			mval.set(wd);

			context.write(mkey, mval);
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

}
