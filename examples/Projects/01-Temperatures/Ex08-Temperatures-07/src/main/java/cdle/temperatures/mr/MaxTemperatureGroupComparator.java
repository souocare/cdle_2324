package cdle.temperatures.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MaxTemperatureGroupComparator 
		extends WritableComparator {
	
	protected MaxTemperatureGroupComparator() {
		super(TemperaturePair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperaturePair tp1 = (TemperaturePair) w1;
		TemperaturePair tp2 = (TemperaturePair) w2;
		
		//return TemperaturePair.compare( ip1.getYear(), ip2.getYear() );
		return tp1.getYear().compareTo( tp2.getYear() );
  }
}