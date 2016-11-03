package cd.mapreduce.wordcount.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper class for word count
 * @author Ravish N
 *
 */
public class MapperImpl extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * map method that splits the words in the file by " " and reads each line
	 * @param key, value, output, reporter
	 */
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		// convert the value to String in order to read the words, iterate through and compare
		String wordRef = value.toString();
		
		// iterate through each word and add them to output collection
		for(String word : wordRef.split(" ")) {
			
			if(word.length() > 0) {
				
				//add each word to the output collection
				output.collect(new Text(word), new IntWritable(1));
			} // end if
		} // end for
	} // end map()
} // end MapperImpl class
