package com.sxt.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWC {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(MyWC.class);
		job.setJobName("sxt-wc");

		Path path = new Path("/user/root/test.txt");
		FileInputFormat.addInputPath(job, path);

		Path output = new Path("/data/wc/output02");
		if (output.getFileSystem(conf).exists(output)) {
			output.getFileSystem(conf).delete(path, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);

		job.waitForCompletion(true);
	}
}
