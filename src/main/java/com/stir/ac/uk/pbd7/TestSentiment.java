package com.stir.ac.uk.pbd7;

import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.*;

import mochadoop.Mochadoop;

public class TestSentiment
{	
	public static void main(String[] args) throws Exception 
	{		
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.OFF);
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Product Sentiment Words");
		
		// The following line will add the file name 'exclude.txt' to the list
		// of cache file names to be used in the mapper. You will
		// need to set the equivalent line in the main method
		// of your actual Map/Reduce main method that will be used on Hadoop.
	    job.addCacheFile(new URI("exclude.txt#first"));

		job.setJarByClass(SentimentWords.class);

		
		// Set mapper class to SentimentMapper defined above
		job.setMapperClass(SentimentWords.SentimentMapper.class);
		
		// Set combine class to SentimentCombiner defined above
		 job.setCombinerClass(SentimentWords.SentimentCombiner.class);
		
		// Set reduce class to SentimentReducer defined above
		job.setReducerClass(SentimentWords.SentimentReducer.class);
		
		// Class of output key is Text
		job.setOutputKeyClass(Text.class);		
		
		// Class of output value is IntWritable
		job.setOutputValueClass(Text.class);
		
		// FileInputFormat.addInputPath(job, new Path("sentiments.txt"));
		FileInputFormat.addInputPath(job, new Path("sentiments.txt"));
		
		// Output path is second argument when program called
		FileOutputFormat.setOutputPath(job, new Path("results.txt"));
		
		Mochadoop mh = new Mochadoop();

		// Set the second parameter to true if you want to see the intermediate
		// input/output from the mapper, combiner and reducer. Set it to false
		// when dealing with larger data sets. The third parameter names 
		// a file to save the debug output to. If this parameter is omitted, the debug output
		// will not be saved. Note that you will need to refresh your Eclipse
		// project view (F5) to see any new files that may have been created by your program.

		mh.runMapReduce(job,true);
//		mh.runMapReduce(job,true,"sentLog.txt");
		// mh.runMapReduce(job);
	}  
}
