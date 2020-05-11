package com.stir.ac.uk.pbd7;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import static java.util.stream.Collectors.*;
import static java.util.Map.Entry.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * Student ID: 2833077
 *
 */

public class SentimentWords {

	// The mapper class
	public static class SentimentMapper extends Mapper<Object, Text, Text, Text> {

		private SentimentParser parser;
		private HashSet<String> exclude;

		/**
		 * Processes a line that is passed to it by writing a key/value pair to the context.
		 *
		 * @param index   A link to the file index
		 * @param value   The line from the file
		 * @param context The Hadoop context object
		 */
		public void map(Object index, Text value, Context context) throws IOException, InterruptedException {
			// Split the text data into it's separate columns using Tab as the delimeter
			parser.parse(value);

			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (String word : parser.getWords()) {
				if (this.exclude.contains(word)) {
					continue;
				}
				context.write(new Text(parser.getCompositeKey()), new Text(word + ":1"));
			}

		}


		/** This method set the global attributes and read the cache file.
		 *
		 * @param context
		 */
		public void setup(Context context) {
			this.parser = new SentimentParser();
			this.exclude = new HashSet<>();
			try {
				//... Get a file name from the cache and read in its contents
				URI[] cacheFiles = context.getCacheFiles();
				for (URI file : cacheFiles) {
					readFile(file, this.exclude);
				}
			} catch (Exception e) {
				System.err.println("Problems setting up mapper: " + e);
			}
		}

		/**
		 * This method read cached file.
		 *
		 * @param file
		 * @throws Exception if cannot read/open the file
		 */
		private void readFile(URI file, HashSet<String> hashSet) throws Exception {
			BufferedReader reader;
			String[] path = file.toString().split("#");
			try {
				reader = new BufferedReader(new FileReader(path[1]));
				String line = null;
				while ((line = reader.readLine()) != null) {
					// read all the lines save in the file and save the unique values in HashSet list.
					List<String> lineAux = Arrays.asList(line.split(","));
					lineAux.replaceAll(String::toLowerCase);
					hashSet.addAll(lineAux);
				}
				reader.close();
			} catch (IOException ex) {
				throw new Exception("File " + file + " was not able to open/read.");
			}
		}
	}

	/**
	 * Sample combiner that just writes back out key/value pairs that the mappers have emitted. This will
	 * need to be changed to make it work effectively.
	 */
	public static class SentimentCombiner extends Reducer<Text, Text, Text, Text> {

		/**
		 * This method will count off all the words in the line received by map.
		 *
		 * @param key key = <itemType> <sentiment> i.e. Movie 0
		 * @param values appended String with word and count values i.e. 'movie:15, time:2....,after:35'
		 * @param context output will be save in the context
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			for (Text value : values) {
				String[] val = value.toString().split(":");
				String wordAsString = new String(val[0]); // Save word key
				int countValue = Integer.parseInt(val[1]); // Save count value equals to 1
				if (map.containsKey(wordAsString)) {
					map.put(wordAsString, map.get(wordAsString) + countValue);
				} else {
					map.put(wordAsString, countValue);
				}
			}
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
				context.write(key, new Text(entry.getKey() + ":" + String.valueOf(entry.getValue())));
			}
		}
	}

	// The Reducer class
	public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {

		HashMap<Text, Integer> counts;

		public void setup(Context context) {
			this.counts = new HashMap<>();
		}

		/**
		 * Reduces multiple data values for a given key into a single output for that key
		 *
		 * @param key     The key that this particular reduce call will be operating on
		 * @param values  An array of values associated with the given key
		 * @param context The Hadoop context object
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			counts.clear();

			// values are iterated [enter:2,admins:1,values:1,wish:3,negative:1,..]
			for (Text value : values) {
				// each value in this format "<word>:<count>" is split.
				String[] val = value.toString().split(":");
				Text word = new Text(val[0]); // Save word key
				int countValue = Integer.parseInt(val[1]); // Save count value
				if (counts.get(word) != null) {
					Integer count = counts.get(word);
					counts.put(word, countValue + count);
				} else {
					counts.put(word, countValue);
				}
			}

			// Emit key and result (i.e word and count).
			context.write(key, new Text(getTopNKeysAsString(counts,5).toString()));
		}

		/**
		 * This method get N top string key values .
		 *
		 * @param map to sort and collect values.
		 * @param n limit the number of keys collected
		 * @return a new String with the top N key values appended
		 */
		private String getTopNKeysAsString(HashMap<Text, Integer> map, int n) {

			Map<String, Integer> mapSorted = new LinkedHashMap<>();
			map.entrySet().stream() // sort map values by desc
					.sorted(Map.Entry.<Text, Integer>comparingByValue().reversed())
					.limit(n) // limited the out to N values
					.forEachOrdered(x -> mapSorted.put(x.getKey().toString(), x.getValue())); // Save the value inside a new Map

			return StringUtils.join(mapSorted.keySet(), " "); // return keys appended as unique String
		}

	}

	  /**
	   * main program that will be run on Hadoop, including configuration setup. You may
	   * want to comment out this method while testing your code on Mochadoop.
	   *
	   * @param args		Command line arguments
	   */
	  public static void main(String[] args) throws Exception
	  {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Product Sentiment Words");

		job.addCacheFile(new URI(args[2]));

	    job.setJarByClass(SentimentWords.class);

	    // Set mapper class to SentimentMapper defined above
	    job.setMapperClass(SentimentMapper.class);

	    // Set combine class to SentimentCombiner defined above
	    job.setCombinerClass(SentimentCombiner.class);

	    // Set reduce class to SentimentReducer defined above
	    job.setReducerClass(SentimentReducer.class);

	    // Class of output key is Text
	    job.setOutputKeyClass(Text.class);

	    // Class of output value is Text
	    job.setOutputValueClass(Text.class);

	    // Input path is first argument when program called
	    FileInputFormat.addInputPath(job, new Path(args[0]));

	    // Output path is second argument when program called
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // waitForCompletion submits the job and waits for it to complete,
	    // parameter is verbose. Returns true if job succeeds.
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}



