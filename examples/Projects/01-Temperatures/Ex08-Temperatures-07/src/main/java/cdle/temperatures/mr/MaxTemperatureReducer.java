package cdle.temperatures.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
		extends Reducer<TemperaturePair, FloatWritable, TemperaturePair, Text> {
	
	private List<String> valuesAsString;
	
	@Override
    public void setup(Context context) 
    		throws IOException, InterruptedException {
		this.valuesAsString = new ArrayList<String>();
	}
  
    @Override
    protected void reduce(TemperaturePair key, Iterable<FloatWritable> values, Context context) 
    		throws IOException, InterruptedException {
    	
    	this.valuesAsString.clear();
    	
    	for (FloatWritable currentTemperature : values) {
    		this.valuesAsString.add(0, "" + currentTemperature.get() );
		}
    	
    	String _out;
    	Text out;
    	
    	switch ( this.valuesAsString.size() ) {
    		case 1:
    			_out = "[" +
						this.valuesAsString.get( 0 ) +
						"]";
    			break;
    			
    		case 2:
    			_out = "[" +
						this.valuesAsString.get( 0 ) + ", " +
						this.valuesAsString.get( 1 )  +
						"]";
    			break;
    			
    		case 3:
    			_out = "[" +
						this.valuesAsString.get( 0 ) + ", " +
						this.valuesAsString.get( 1 ) + ", " +
						this.valuesAsString.get( 2 )  +
						"]";
    			break;
    			
    		case 4:
    			_out = "[" +
						this.valuesAsString.get( 0 ) + ", " +
						this.valuesAsString.get( 1 ) + ", " +
						this.valuesAsString.get( 2 ) + ", " +
						this.valuesAsString.get( 3 )  +
						"]";
    			break;
    			
    		default:
    			_out = "[" +
						this.valuesAsString.get( 0 ) + ", " +
						this.valuesAsString.get( 1 ) + ", " +
						"... " + ", " +
						this.valuesAsString.get( this.valuesAsString.size()/2 ) + ", " +
						"... " + ", " +
						this.valuesAsString.get( this.valuesAsString.size()-2 ) + ", " +
						this.valuesAsString.get( this.valuesAsString.size()-1 ) +
						"]";    			
    			break;
    	}
    	
    	out = new Text( String.format( "{%d temperatures, %s}", this.valuesAsString.size(), _out) );
    	
    	context.write( key, out );
    }
  }