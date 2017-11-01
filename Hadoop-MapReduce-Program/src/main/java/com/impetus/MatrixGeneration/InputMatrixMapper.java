package com.impetus.MatrixGeneration;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputMatrixMapper extends Mapper<LongWritable, Text, CustomerProductIndexPair, Text> {

	private Text val = new Text();
	private CustomerProductIndexPair custProdPair = new CustomerProductIndexPair();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		custProdPair.setCustIndx(Integer.parseInt(line[19]));
		custProdPair.setProdIndx(Integer.parseInt(line[20]));
		if (Double.parseDouble(line[4]) > 0.0) {
			val.set(custProdPair.getProdIndx() + "," + 1);
		} else {
			val.set(custProdPair.getProdIndx() + "," + 0);
		}
		context.write(custProdPair, val);
	}

}
