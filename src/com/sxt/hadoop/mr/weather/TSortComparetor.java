package com.sxt.hadoop.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TSortComparetor extends WritableComparator {

	public TSortComparetor() {

		super(TQ.class, true);

	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		TQ t1 = (TQ) a;
		TQ t2 = (TQ) b;
		int c1 = Integer.compare(t1.getYear(), t2.getYear());
		if (c1 == 0) {

			int c2 = Integer.compare(t1.getMonth(), t2.getMonth());
			if (c2 == 0) {

				return Integer.compare(t1.getTemperature(), t2.getTemperature());
			}

			return c2;
		}

		return c1;
	}
}
