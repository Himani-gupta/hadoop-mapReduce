package com.impetus.MatrixGeneration;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InputMatrixReducer extends Reducer<CustomerProductIndexPair, Text, LongWritable, Text> {

	private List<Integer> prodIndxList = new ArrayList<Integer>();

	@Override
	protected void setup(Reducer<CustomerProductIndexPair, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		loadProductInfo(context);
	}

	private void loadProductInfo(Reducer<CustomerProductIndexPair, Text, LongWritable, Text>.Context context) {
		try {
			BufferedReader br = new BufferedReader(new FileReader("productIndexes"));
			String line;
			while ((line = br.readLine()) != null) {
				prodIndxList.add(Integer.parseInt(line.toString()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void reduce(CustomerProductIndexPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		LongWritable custIndx = new LongWritable(key.getCustIndx());
		Text ratingMatrix = new Text();
		Map<Integer, Integer> prodIndxRatingMap = new HashMap<Integer, Integer>();
		List<Integer> ratingList = new ArrayList<Integer>();

		while (itr.hasNext()) {
			String token[] = itr.next().toString().split(",");
			prodIndxRatingMap.put(Integer.parseInt(token[0]), Integer.parseInt(token[1]));
		}

		for (int i = 0; i < prodIndxList.size(); i++) {
			if (prodIndxRatingMap.get(prodIndxList.get(i)) != null) {
				ratingList.add(prodIndxRatingMap.get(prodIndxList.get(i)));
			} else {
				ratingList.add(0);
			}
		}
		ratingMatrix.set(StringUtils.join(ratingList, ' '));
		context.write(custIndx, ratingMatrix);
	}
}
