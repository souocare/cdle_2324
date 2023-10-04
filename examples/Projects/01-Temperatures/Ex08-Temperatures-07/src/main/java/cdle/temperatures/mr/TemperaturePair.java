package cdle.temperatures.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class TemperaturePair 
		implements WritableComparable<TemperaturePair> {

	private IntWritable year;
	
	public IntWritable getYear() {
		return this.year;
	}
	
	public void setYear(IntWritable year) {
		this.year = year;
	}
	
	public void setYear(int year) {
		this.year.set( year );
	}
	
	private FloatWritable temperature;
	
	public FloatWritable getTemperature() {
		return this.temperature;
	}
	
	public void setTemperature(FloatWritable temperature) {
		this.temperature = temperature;
	}
	
	public void setTemperature(float temperature) {
		this.temperature.set( temperature );
	}

	public TemperaturePair() {
		this.set(new IntWritable(), new FloatWritable() );
	}

	public TemperaturePair(IntWritable year, FloatWritable temperature) {
		this.set( year, temperature);
	}
	
	public void set(IntWritable year, FloatWritable temperature) {
		this.year = year;
		this.temperature = temperature;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.year.write( out );
		this.temperature.write( out );
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year.readFields( in );
		this.temperature.readFields( in );
	}

	@Override
	public int hashCode() {
		return this.year.hashCode() * 163 + this.temperature.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof TemperaturePair) {
			TemperaturePair tp = (TemperaturePair)o;
			return this.year.equals( tp.year ) && this.temperature.equals( tp.temperature );
		}
		return false;
	}

	@Override
	public String toString() {
		return this.year + "\t" + this.temperature;
	}

	@Override
	public int compareTo(TemperaturePair tp) {
		int cmp = this.year.compareTo( tp.year );
		if (cmp != 0) {
			return cmp;
		}
		return this.temperature.compareTo( tp.temperature );
	}
}