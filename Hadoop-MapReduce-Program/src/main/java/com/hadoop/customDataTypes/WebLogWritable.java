package com.hadoop.customDataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WebLogWritable implements WritableComparable<WebLogWritable> {

	private Text siteURL, reqDate, timestamp, ipaddr;
	private IntWritable reqNo;

	public WebLogWritable() {
		this.siteURL = new Text();
		this.reqDate = new Text();
		this.timestamp = new Text();
		this.ipaddr = new Text();
		this.reqNo = new IntWritable();
	}

	public WebLogWritable(Text siteURL, Text reqDate, Text timestamp, Text ipaddr, IntWritable reqNo) {

		this.siteURL = siteURL;
		this.reqDate = reqDate;
		this.timestamp = timestamp;
		this.ipaddr = ipaddr;
		this.reqNo = reqNo;
	}

	public void set(Text siteURL, Text reqDate, Text timestamp, Text ipaddr, IntWritable reqNo) {

		this.siteURL = siteURL;
		this.reqDate = reqDate;
		this.timestamp = timestamp;
		this.ipaddr = ipaddr;
		this.reqNo = reqNo;
	}

	public Text getIp() {
		return ipaddr;
	}

	public void write(DataOutput out) throws IOException {

		ipaddr.write(out);
		timestamp.write(out);
		reqDate.write(out);
		reqNo.write(out);
		siteURL.write(out);
	}

	public void readFields(DataInput in) throws IOException {

		ipaddr.readFields(in);
		timestamp.readFields(in);
		reqDate.readFields(in);
		reqNo.readFields(in);
		siteURL.readFields(in);
	}

	public int compareTo(WebLogWritable o) {
		// TODO Auto-generated method stub
		if (ipaddr.compareTo(o.ipaddr) == 0) {
			return timestamp.compareTo(o.timestamp);
		}
		return ipaddr.compareTo(o.ipaddr);
	}

	/*public boolean equals(Object o) {
		if (o instanceof WebLogWritable) {
			WebLogWritable other = (WebLogWritable) o;
			return ipaddr.equals(other.ipaddr) && timestamp.equals(other.timestamp);
		}
		return false;
	}

	public int hashCode() {
		return ipaddr.hashCode();
	}*/
}
