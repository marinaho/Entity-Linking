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

public class ExtractWikiArticleTitles extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractWikiArticleTitles.class);
	
	public static class Map extends MapReduceBase implements 
			Mapper<IntWritable, WikipediaPage, Text, Text> {
		Text outputValue = new Text();
		
		// Emit: key = article title, filter out redirects, disambiguation and empty pages.
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
	
	
	public void task(Configuration config, String inputPath, String outputPath) throws IOException {
		LOG.info("Exracting article titles: filter out redirects, disambiguation, category and list "
				+ "pages...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(config, ExtractWikiArticleTitles.class);
		conf.setJobName(
				String.format(
						"WikipediaProcessing: extractPrimaryArticleTitlesTask[input: %s, output: %s]", 
						inputPath, 
						outputPath
				)
		);
		conf.setJarByClass(ExtractWikiArticleTitles.class);

		conf.setNumReduceTasks(0);

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

		task(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, tmp)
		);

		return 0;
	}
	
	public ExtractWikiArticleTitles() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractWikiArticleTitles(), args);
	}
}
