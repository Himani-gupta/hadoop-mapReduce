package com.impetus.MatrixGeneration;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomerGroupComparator extends WritableComparator {

	protected CustomerGroupComparator() {
		super(CustomerProductIndexPair.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomerProductIndexPair key1 = (CustomerProductIndexPair) w1;
		CustomerProductIndexPair key2 = (CustomerProductIndexPair) w2;
		return key1.getCustIndx().compareTo(key2.getCustIndx());
	}

}
