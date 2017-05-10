package com.hive;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class SortNumbersUDF extends UDF {
	public static Text evaluate(Text str, Text delim) {
		String[] numbers = str.toString().split(delim.toString());
		if (numbers.length == 0)
			return str; // return the original
						// string as is
		ArrayList<Integer> num_array = new ArrayList<Integer>();
		for (int i = 0; i < numbers.length; i++)
			num_array.add(new Integer(Integer.parseInt(numbers[i]))); 
		Collections.sort(num_array); // well, that was easy. Sort all Integers
		StringBuilder sb = new StringBuilder(); // optimized for String concat
		for (Integer s : num_array) {
			sb.append(Integer.toString(s));
			sb.append(delim.toString()); // note, an extra delim would appear
		} // after the last string is appended

		return new Text(sb.toString().substring(0, sb.toString().length() - 1));
	}
}
