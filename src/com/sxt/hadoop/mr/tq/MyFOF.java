package com.sxt.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyFOF {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);

		job.setJarByClass(MyFOF.class);

		Path in = new Path("/data/fof/input");
		FileInputFormat.addInputPath(job, in);
		Path out = new Path("/data/fof/output");
		if (out.getFileSystem(conf).exists(out)) {
			out.getFileSystem(conf).delete(out, true);
		}
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(FMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(FReduce.class);
		job.waitForCompletion(true);

	}
}
