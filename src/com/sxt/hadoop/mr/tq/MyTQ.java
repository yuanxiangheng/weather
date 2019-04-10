package com.sxt.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTQ {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration(true);
		Job job = Job.getInstance(conf);

		Path input = new Path("/data/tq/input");
		FileInputFormat.addInputPath(job, input);
		Path output = new Path("/data/tq/output");
		if (output.getFileSystem(conf).exists(output)) {
			output.getFileSystem(conf).delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);

		job.setMapperClass(TMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setPartitionerClass(Tpartitioner.class);
		job.setSortComparatorClass(TSortComparetor.class);

		job.setCombinerClass(TCombiner.class);
		job.setCombinerKeyGroupingComparatorClass(TGroupingComparator.class);

		job.setGroupingComparatorClass(TGroupingComparator.class);
		job.setReducerClass(TReducer.class);

		job.waitForCompletion(true);
	}

}
