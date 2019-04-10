package com.sxt.hadoop.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupingComparator extends WritableComparator {

	public TGroupingComparator() {

		super(TQ.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ t1 = (TQ) a;
		TQ t2 = (TQ) b;
		int c1 = Integer.compare(t1.getYear(), t2.getYear());
		if (c1 == 0) {

			return Integer.compare(t1.getMonth(), t2.getMonth());
		}

		return c1;
	}
}
