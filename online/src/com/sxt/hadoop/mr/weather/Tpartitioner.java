package com.sxt.hadoop.mr.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class Tpartitioner extends Partitioner<TQ, IntWritable> {

	@Override
	public int getPartition(TQ key, IntWritable value, int numPartitions) {

		return key.getYear() % numPartitions;

	}

}
