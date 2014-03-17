package knowledgebase;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/* 
 * Pipeline that extracts a list of wikipedia article titles from wikipedia dump repacked in 
 * sequence file format.
 * 1. Run steps 1-2 & 5 from ExtractEntityMentionPipeline.
 * 2. Run: 
 * 			hadoop WikiPipeline.jar knowledgebase.ExtractWikiArticleTitles \
 * 				-input enwiki-latest.block -output /enwiki-titles.txt 
 */
public class WikiArticleTitlesIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WikiArticleTitlesIndexBuilder.class);
	
	public static class Map extends MapReduceBase implements 
			Mapper<IntWritable, WikipediaPage, Text, Text> {
		Text outputValue = new Text();
		
		// Emit: key = article title, filter out redirects, disambiguation, list and category pages.
		public void map(IntWritable key, WikipediaPage p, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			if (p.isRedirect() || p.isDisambiguation() || WikiUtils.isCategoryPage(p.getTitle()) ||
					WikiUtils.isListPage(p.getTitle())) 
				return;
			if (p.isArticle()) {
				outputValue.set(p.getDocid());
				output.collect(new Text(p.getTitle()), outputValue);
			}
		}
	}
	
	
	public void task(Configuration config, String inputPath, String outputPath, int num_reducers) 
			throws IOException {
		LOG.info("Extracting article titles: filter out redirects, disambiguation, category and list "
				+ "pages...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(config, WikiArticleTitlesIndexBuilder.class);
		conf.setJobName(
				String.format(
						"WikipediaProcessing: extractPrimaryArticleTitlesTask[input: %s, output: %s]", 
						inputPath, 
						outputPath
				)
		);
		conf.setJarByClass(WikiArticleTitlesIndexBuilder.class);

		conf.setNumReduceTasks(num_reducers);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}

	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	private static final String NUM_REDUCERS_OPTION = "num_reducers";
	
	private static final int DEFAULT_NUM_REDUCERS = 1;
	
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("wikipedia sequence file").isRequired().create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("output").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("num_reducers")
				.hasArg().withDescription("number of reducers").create(NUM_REDUCERS_OPTION));

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

		Integer num_reducers = 
				cmdline.hasOption(NUM_REDUCERS_OPTION) ? 
						Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS_OPTION)) : 
						DEFAULT_NUM_REDUCERS;
						
		task(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, tmp),
				num_reducers
		);

		return 0;
	}
	
	public WikiArticleTitlesIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WikiArticleTitlesIndexBuilder(), args);
	}
}
