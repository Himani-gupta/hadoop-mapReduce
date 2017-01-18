package com.hadoop.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountComparator extends WritableComparator {

	protected WordCountComparator() {
		super(Text.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text key1 = (Text) a;
		Text key2 = (Text) b;

		// Implement sorting in descending order
		System.out.println("errrrrrrrrrrrrrrrrrrrrrrrrrrrr" + key1.compareTo(key2));
		return -1 * key1.compareTo(key2);
	}

	/*@Override
	public int compare(Object a, Object b) {
		Text key1 = (Text) a;
		Text key2 = (Text) b;

		// Implement sorting in descending order
		System.out.println("errrrrrrrrrrrrrrrrrrrrrrrrrrrr" + key1.compareTo(key2));
		return -1 * key1.compareTo(key2);
	}
*/
}
