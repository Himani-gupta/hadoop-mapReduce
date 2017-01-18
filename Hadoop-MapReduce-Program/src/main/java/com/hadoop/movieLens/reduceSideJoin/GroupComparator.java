package com.hadoop.movieLens.reduceSideJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

	protected GroupComparator() {
		super(CustomKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CustomKey key1 = (CustomKey) w1;
		CustomKey key2 = (CustomKey) w2;
		return key1.getMovieId().compareTo(key2.getMovieId());
	}

}
