package knowledgebase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import normalizer.Normalizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * Constructs an index of pairs (term, document frequency).
 * Only single word terms are considered. Punctuation is removed.
 * This can be loaded into memory by using @See index.TermDocumentFrequencyIndex .
 */
public class DFTermIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(DFTermIndexBuilder.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };

  public static class Map extends MapReduceBase 
  		implements Mapper<IntWritable, WikipediaPage, Text, IntWritable> {
  	private static final Text outputKey = new Text();
  	private static final IntWritable outputOne = new IntWritable(1);

  	/**
  	 *  Emits:	key = term, value = 1
  	 */
  	@Override
  	public void map(IntWritable key, WikipediaPage page, OutputCollector<Text, IntWritable> output, 
  			Reporter reporter) throws IOException {
  		if (!page.isArticle()) {
  			return;
  		}
  		reporter.incrCounter(Counters.PAGES_TOTAL, 1);
  		
  		String normalizedContent = Normalizer.normalizeNoDelimiters(page.getContent());
  		String tokens[] = StringUtils.split(normalizedContent, Normalizer.WHITESPACES);
  		Set<String> termSet = new HashSet<String>(Arrays.asList(tokens));

  		for (String term: termSet) {
  			outputKey.set(term);
  			output.collect(outputKey, outputOne);
  		}
  	}
  }

  /**
   * Emits: key = term, value = document frequency
   * Used as combiner (for efficiency) and reducer.
   */
  public static class Reduce extends MapReduceBase 
  		implements Reducer<Text, IntWritable, Text, IntWritable> {
  	private static final IntWritable outputValue = new IntWritable();
  	
  	@Override
  	public void reduce(Text key, Iterator<IntWritable> values,
  			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {		
  		int total = 0;
  		while (values.hasNext()) {
  			total += values.next().get();
  		}	
  		outputValue.set(total);
  		output.collect(key, outputValue);
  	}		
  }

	private static final String INPUT_OPTION = "input";
	private static final String NUM_REDUCERS_OPTION = "num_reducers";
	private static final String OUTPUT_OPTION = "output";
	
	private static final int DEFAULT_NUM_REDUCERS = 1;

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("wikipedia sequence file").isRequired().create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("num_reducers")
				.hasArg().withDescription("number of reducers").create(NUM_REDUCERS_OPTION));
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("output").create(OUTPUT_OPTION));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(this.getClass().getName(), options);
			return -1;
		}

		Integer num_reducers = 
				cmdline.hasOption(NUM_REDUCERS_OPTION) ? 
						Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS_OPTION)) : 
						DEFAULT_NUM_REDUCERS;
						
		Random random = new Random();
		String defaultOutput = "tmp-" + this.getClass().getCanonicalName() + "-" + 
				random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, defaultOutput),
				num_reducers
		);

		return 0;
	}
	
	public void task1(Configuration config, String inputPath, String outputPath, int num_reducers) 
			throws IOException, URISyntaxException {
		LOG.info("Extracting document frequency index...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);


		JobConf conf = new JobConf(config, DFTermIndexBuilder.class);
		conf.setJobName(String.format(
				"DFTermIndexBuilder:[input: %s, output: %s]", 
				inputPath, 
				outputPath
				)
		);
		conf.setJarByClass(DFTermIndexBuilder.class);

		conf.setNumReduceTasks(num_reducers);
		
		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		JobClient.runJob(conf);
	}
	
	public DFTermIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DFTermIndexBuilder(), args);
	}
}
