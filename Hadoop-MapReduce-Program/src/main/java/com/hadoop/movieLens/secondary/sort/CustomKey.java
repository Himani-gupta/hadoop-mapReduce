package com.hadoop.movieLens.secondary.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class CustomKey implements WritableComparable<CustomKey>{
	
	private Integer movieId;
	private Integer rating;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(movieId);
		out.writeInt(rating);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		movieId = in.readInt();
		rating = in.readInt();
	}

	@Override
	public int compareTo(CustomKey o) {
		if (movieId.compareTo(o.getMovieId()) == 0) {
			return -1 * rating.compareTo(o.getRating());
		}
		return movieId.compareTo(o.getMovieId());
	}

	@Override
	public int hashCode() {
		return movieId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomKey other = (CustomKey) obj;
		if (this.movieId.equals(other.getMovieId())) {
			return this.rating.equals(other.getRating());
		} else {
			return this.movieId.equals(other.getMovieId());
		}
	}

	public Integer getMovieId() {
		return movieId;
	}

	public void setMovieId(Integer movieId) {
		this.movieId = movieId;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer dataType) {
		this.rating = dataType;
	}
	
}
