package cdle.temperatures.mr;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class MaxTemperatureMapperTest {
	@Test
	public void processesValidRecordVer01()
			throws IOException, InterruptedException {
		Text value = new Text( "0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999" );
		//                               Year ^^^^                                                        Temperature ^^^^^

		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper( new MaxTemperatureMapperVer01() )
			.withInput( new LongWritable(0), value)
			.withOutput(new Text("1950"), new IntWritable(-11))
			.runTest();
	}

	@Test
	public void processesValidRecordVer02()
			throws IOException, InterruptedException {
		Text value = new Text( "0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999" );
		//                               Year ^^^^                                                        Temperature ^^^^^

		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper( new MaxTemperatureMapperVer02() )
			.withInput( new LongWritable(0), value)
			.withOutput(new Text("1950"), new IntWritable(-11))
			.runTest();
	}
	
	@Test
	public void ignoresMissingTemperatureRecord() 
			throws IOException, InterruptedException {
	    Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999" );
	    //                               Year ^^^^                                                        Temperature ^^^^^

	    new MapDriver<LongWritable, Text, Text, IntWritable>()
	      .withMapper(new MaxTemperatureMapperVer02() )
	      .withInput(new LongWritable(0), value)
	      .runTest();
	  }
	
	@Test
	public void returnsMaximumIntegerInValues()
			throws IOException, InterruptedException {
		
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
			.withReducer(new MaxTemperatureReducer() )
			.withInput(
					new Text("1950"),
					Arrays.asList(new IntWritable(10), new IntWritable(5)))
			.withOutput(new Text("1950"), new IntWritable(10))
			.runTest();
	}
	
	/*
	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
	    conf.set( "fs.defaultFS", "file:/// ");
	    conf.set( "mapreduce.framework.name", "local" );
	    conf.setInt( "mapreduce.task.io.sort.mb", 1 );
	    
	    Path input = new Path( "input/ncdc/micro" );
	    Path output = new Path( "output/ncdc/micro" );
	    
		FileSystem fs = FileSystem.getLocal(conf);
	    
	    // delete old output
	    fs.delete(output, true);
	    
	    MaxTemperatureDriver driver = new MaxTemperatureDriver();
	    driver.setConf( conf );
	    
	    int exitCode = driver.run(new String[] { input.toString(), output.toString() } );
	    assertThat(exitCode, is(0) );
	    
	    //checkOutput(conf, output);
	}
	*/
}