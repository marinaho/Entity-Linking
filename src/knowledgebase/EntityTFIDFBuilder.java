package knowledgebase;

import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import data.TFIDFEntry;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * Computes a mapping from an entity id to a vector of pairs (term, tf-idf score).
 * The mapping is spread over multiple files.
 * This mapping is at best not meant to be loaded in memory because of it's large size. 
 * As the output is a sequence file, an index of the positions of each entity in the file can be 
 * computed. Then a tf-idf vector of an entity can be read from disk by seeking at the corresponding
 * offset. 
 * To accomplish this another index must be constructed: @See EntityTFIDFIndexBuilder .
 */
public class EntityTFIDFBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EntityTFIDFBuilder.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };

  public static class TFIDFMap extends MapReduceBase 
  		implements Mapper<IntWritable, WikipediaPage, IntWritable, TFIDFEntry> {
  	private static final IntWritable outputKey = new IntWritable();
  	private static TermDocumentFrequencyIndex dfIndex;

		@Override
		public void configure(JobConf job) {
			String dfIndexPath = job.get(DF_INDEX_SYMLINK);
			try {
				dfIndex = TermDocumentFrequencyIndex.load(dfIndexPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
  	/**
  	 *  Emits:	key = doc id, value = TFIDFEntry containing pairs (term, tf-idf score).
  	 */
  	@Override
  	public void map(IntWritable key, WikipediaPage page, 
  			OutputCollector<IntWritable, TFIDFEntry> output, Reporter reporter) throws IOException {
  		if (!page.isArticle()) {
  			return;
  		}
  		reporter.incrCounter(Counters.PAGES_TOTAL, 1);
  		String normalizedContent = Normalizer.normalizeNoDelimiters(page.getContent());
  		String[] terms = StringUtils.split(normalizedContent, Normalizer.WHITESPACES);
  		HashMap<String, Integer> termFreq = new HashMap<String, Integer>();

  		for (String term: terms) {
  			if (termFreq.containsKey(term)) {
  				termFreq.put(term, termFreq.get(term) + 1);
  			} else {
  				termFreq.put(term, 1);
  			}
  		}
  		
  		TFIDFEntry outputValue = new TFIDFEntry(termFreq.size());
  		for (Map.Entry<String, Integer> entry : termFreq.entrySet()) {
  			String term = entry.getKey();
  			int tf = entry.getValue();
  			double idf = dfIndex.getIDF(term);
  			outputValue.put(term, tf * idf);
  		}
  		
			outputKey.set(Integer.parseInt(page.getDocid()));
			output.collect(outputKey, outputValue);
  	}
  }

	private static final String INPUT_OPTION = "input";
	private static final String DF_INDEX_OPTION = "df_index";
	private static final String OUTPUT_OPTION = "output";
	private static final String DEFAULT_DF_INDEX = "/df-index.txt";
	private static final String DF_INDEX_SYMLINK = "df";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("wikipedia sequence file").isRequired().create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("output").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("term document frequency index file").create(DF_INDEX_OPTION));

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
		String defaultOutput = "output-" + this.getClass().getCanonicalName() + "-" + 
				random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION),
				cmdline.getOptionValue(DF_INDEX_OPTION, DEFAULT_DF_INDEX),
				cmdline.getOptionValue(OUTPUT_OPTION, defaultOutput)
		);

		return 0;
	}
	
	@SuppressWarnings("deprecation")
	public void task1(Configuration config, String inputPath, String dfIndexPath, String outputPath) 
			throws IOException, URISyntaxException {
		LOG.info("Extracting TF-IDF vectors for wiki articles...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - df index: " + dfIndexPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(config, EntityTFIDFBuilder.class);
		conf.setJobName(String.format(
				"EntityTFIDFBuilder:[input: %s, df index: %s, output: %s]", 
				inputPath, 
				dfIndexPath,
				outputPath
				)
		);
		conf.setJarByClass(EntityTFIDFBuilder.class);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(TFIDFEntry.class);
		
		conf.setMapperClass(TFIDFMap.class);
		
		conf.setNumReduceTasks(0);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(dfIndexPath + "#" + DF_INDEX_SYMLINK), conf);
		conf.set(DF_INDEX_SYMLINK, DF_INDEX_SYMLINK);

		JobClient.runJob(conf);
	}
	
	public EntityTFIDFBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new EntityTFIDFBuilder(), args);
	}
}
