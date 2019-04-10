package com.sxt.hadoop.mr.weather;

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

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date data = sdf.parse(strs[0]);

			Calendar calendar = Calendar.getInstance();
			calendar.setTime(data);
			mkey.setYear(calendar.get(Calendar.YEAR));
			mkey.setMonth(calendar.get(Calendar.MONTH) + 1);
			mkey.setDay(calendar.get(Calendar.DAY_OF_MONTH));

			String substring = strs[1].substring(0, strs[1].length() - 1);
			int temperature = Integer.parseInt(substring);

			mkey.setTemperature(temperature);
			mval.set(temperature);

			context.write(mkey, mval);

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

}
