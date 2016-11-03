package cd.mapreduce.wordcount.main;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cd.mapreduce.wordcount.mapper.MapperImpl;
import cd.mapreduce.wordcount.reducer.ReducerImpl;

/**
 * Main class with word count implementation and to run the MapReduce job
 * @author Ravish N
 *
 */
public class WordCount extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if(args.length < 2) {
			
			System.out.println("Invalid input/output directory");
			return -1;
		}
		// Create a job configuraiton for the WordCount class
		JobConf config = new JobConf(WordCount.class);
		
		// Identify input and output files for read/write operations
		FileInputFormat.setInputPaths(config, new Path(args[0]));
		FileOutputFormat.setOutputPath(config, new Path(args[1]));
		
		// Set mapper and reducer classes
		config.setMapperClass(MapperImpl.class);
		config.setReducerClass(ReducerImpl.class);
		
		// Identify and set the class types for key and value
		config.setMapOutputKeyClass(Text.class);
		config.setMapOutputValueClass(IntWritable.class);
		config.setOutputKeyClass(Text.class);
		config.setOutputValueClass(IntWritable.class);
		
		// run MapReduce job
		JobClient.runJob(config);
		
		return 0;
	} // end of run()
	
	/**
	 * main method to run the word count job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	} // end of main()

} // end of WordCount class
