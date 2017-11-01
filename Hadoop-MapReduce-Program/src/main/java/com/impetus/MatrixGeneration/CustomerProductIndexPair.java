package com.impetus.MatrixGeneration;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class CustomerProductIndexPair implements WritableComparable<CustomerProductIndexPair> {

	private Integer custIndx;
	private Integer prodIndx;

	public void write(DataOutput out) throws IOException {
		out.writeInt(custIndx);
		out.writeInt(prodIndx);
	}

	public void readFields(DataInput in) throws IOException {
		custIndx = in.readInt();
		prodIndx = in.readInt();
	}

	public int compareTo(CustomerProductIndexPair o) {
		if (custIndx.compareTo(o.getCustIndx()) == 0) {
			return prodIndx.compareTo(o.getProdIndx());
		}
		return custIndx.compareTo(o.getCustIndx());
	}

	@Override
	public int hashCode() {
		return custIndx.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomerProductIndexPair other = (CustomerProductIndexPair) obj;
		if (this.custIndx.equals(other.getCustIndx())) {
			return this.prodIndx.equals(other.getProdIndx());
		} else {
			return this.custIndx.equals(other.getCustIndx());
		}
	}

	public Integer getCustIndx() {
		return custIndx;
	}

	public void setCustIndx(Integer custIndx) {
		this.custIndx = custIndx;
	}

	public Integer getProdIndx() {
		return prodIndx;
	}

	public void setProdIndx(Integer prodIndx) {
		this.prodIndx = prodIndx;
	}
}
