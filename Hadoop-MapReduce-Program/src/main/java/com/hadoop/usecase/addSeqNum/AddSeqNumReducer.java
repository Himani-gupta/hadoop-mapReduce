package com.hadoop.usecase.addSeqNum;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AddSeqNumReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	private LongWritable updatedSeqNo = new LongWritable();
	private Text empData = new Text();


	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		List<String> sortedDataList = new ArrayList<String>();
		for (Text value : values) {
			sortedDataList.add(value.toString());
		}
		System.out.println("reducer keyyyyyyyyyyyyyyyy "+key);
		Collections.sort(sortedDataList, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				String offset1 = o1.split(" ")[0];
				String offset2 = o2.split(" ")[0];
				if (Long.parseLong(offset1) < Long.parseLong(offset2)) {
					return -1;
				} else if (Long.parseLong(offset1) > Long.parseLong(offset2)) {
					return 1;
				}
				return 0;
			}

		});

		for (int i = 0; i < sortedDataList.size(); i++) {
			String val[] = sortedDataList.get(i).toString().split(" ");
			empData.set(val[1]);
			//empData.set((i * 10) + key.get() +","+val[1]);
			updatedSeqNo.set((i * 10) + key.get());
			context.write(updatedSeqNo, empData);
		}

	}
}
