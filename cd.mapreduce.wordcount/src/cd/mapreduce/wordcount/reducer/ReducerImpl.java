package cd.mapreduce.wordcount.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer class for word count
 * @author Ravish N
 *
 */
public class ReducerImpl extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * reduce method that iterates through the values and counts the number of occurances
	 * of each word in the input file
	 * @param key, values, output, reporter
	 */
	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		// count of each word's occurance
		int count = 0;
		
		// iterate through the values collection and count the number of occurances of each word in the input file
		while(values.hasNext()) {
			
			IntWritable i = values.next();
			// increment count when the value repeats 
			count += i.get();
		} // end while
		
		// store the keys and values to output collection
		output.collect(key, new IntWritable(count));
	} // end reduce()
} // end ReducerImpl class
