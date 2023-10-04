package cdle.temperatures.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MaxTemperatureKeyComparator 
		extends WritableComparator {

	protected MaxTemperatureKeyComparator() {
		super(TemperaturePair.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		TemperaturePair tp1 = (TemperaturePair)w1;
		TemperaturePair tp2 = (TemperaturePair)w2;
		
		//int cmp = TemperaturePair.compare( tp1.getYear(), tp2.getYear() );
		int cmp = tp1.getYear().compareTo( tp2.getYear() );
		
		if ( cmp!=0 ) {
			return cmp;
	    }
		//reverse
	    //return -TemperaturePair.compare( tp1.getTemperature(), tp2.getTemperature() );
		return -tp1.getTemperature().compareTo( tp2.getTemperature() );
	}
}
