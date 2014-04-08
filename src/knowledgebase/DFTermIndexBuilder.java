package knowledgebase;

import java.io.IOException;
import java.net.URISyntaxException;
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

public class DFTermIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(DFTermIndexBuilder.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };

  public static class Map extends MapReduceBase 
  		implements Mapper<IntWritable, WikipediaPage, Text, IntWritable> {
  	private static Text outputKey = new Text();
  	private static IntWritable outputOne = new IntWritable(1);

  	/*
  	 *  Emits:	key = term, value = 1
  	 */
  	@Override
  	public void map(IntWritable key, WikipediaPage page, 
  			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
  		if (!page.isArticle()) {
  			return;
  		}
  		reporter.incrCounter(Counters.PAGES_TOTAL, 1);
  		String normalizedContent = Normalizer.normalizeNoDelimiters(page.getContent());
  		String tokens[] = StringUtils.split(normalizedContent, " \t\f\n\r");
  		Set<String> termSet = new HashSet<String>();
 
  		for (String token: tokens) {
  			termSet.add(token);
  		}
  		
  		for (String term: termSet) {
  			outputKey.set(term);
  			output.collect(outputKey, outputOne);
  		}
  	}
  }

  public static class Reduce extends MapReduceBase 
  		implements Reducer<Text, IntWritable, Text, IntWritable> {
  	IntWritable outputValue = new IntWritable();
  	
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
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("wikipedia sequence file").isRequired().create(INPUT_OPTION));
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

		Random random = new Random();
		String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, tmp)
		);

		return 0;
	}
	
	public void task1(Configuration config, String inputPath, String outputPath) 
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

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
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
