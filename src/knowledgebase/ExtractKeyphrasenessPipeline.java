package knowledgebase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import knowledgebase.WikiPipeline.PageTypes;
import normalizer.Normalizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.io.pair.PairOfInts;

public class ExtractKeyphrasenessPipeline extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractEntityMentionPipeline.class);

	private static final String INPUT_OPTION = "input";
	private static final String NUM_REDUCERS_OPTION = "num_reducers";
	private static final String OUTPUT_OPTION = "output";
	private static final String ANCHOR_TEXT_INDEX_OPTION = "anchor_text";

	private static final int DEFAULT_NUM_REDUCERS = 10;
	private static final String DEFAULT_ANCHOR_TEXT_FILE = "mention_entity_index";
	
	static final String ANCHOR_TEXT_INDEX_SYMLINK = "anchor_file";
	
	private static final int NGRAM_SIZE = 8;

	public static class Map extends MapReduceBase implements
			Mapper<IntWritable, WikipediaPage, Text, IntWritable> {
		private static Text outputKey = new Text();
		private static IntWritable outputValue = new IntWritable();
		private static WikipediaAnchorTextIndex anchorTextSet;

		@Override
		public void configure(JobConf job) {
			String anchorTextPath = job.get(ANCHOR_TEXT_INDEX_SYMLINK);
			try {
				anchorTextSet = WikipediaAnchorTextIndex.load(anchorTextPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/*
		 *  Emits:	key = mention, value = 0, if mention is not linked
		 *  				key = mention, value = 1, if mention is linked
		 */
		@Override
		public void map(IntWritable key, WikipediaPage p, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(PageTypes.TOTAL, 1);

			String normalizedContent = Normalizer.normalize(p.getContent());
			StringTokenizer tokenizer = new StringTokenizer(normalizedContent);
			Set<String> set = new HashSet<String>();
			
			StringBuilder ngrams[] = new StringBuilder[NGRAM_SIZE];
			for (int i = 0; i < NGRAM_SIZE && tokenizer.hasMoreTokens(); ++i) {
				ngrams[i] = new StringBuilder("");
				String next = tokenizer.nextToken();
				for(int j = 0; j <= i; ++j) {
					ngrams[j].append(next);
					if (anchorTextSet.contains(ngrams[j].toString())) {
						set.add(ngrams[j].toString());
					}
				}
			}
			
			int start = 0;
			while (tokenizer.hasMoreTokens()) {
				String next = tokenizer.nextToken();
				ngrams[start] = new StringBuilder(next);
				if (anchorTextSet.contains(next)) {
					set.add(next);
				}
				
				for (int i = (start + 1) % NGRAM_SIZE; i != start; i = (i + 1) % NGRAM_SIZE) {
					ngrams[i].append(next);
					if (anchorTextSet.contains(ngrams[i].toString())) {
						set.add(ngrams[i].toString());
					}
				}
			}
			
			for (Link link : p.extractLinks()) {	
				String normalizedAnchorText = Normalizer.processAnchorText(link.getAnchorText());
				if (set.contains(normalizedAnchorText)) {
					outputKey.set(normalizedAnchorText);
					outputValue.set(1);
					output.collect(outputKey, outputValue);
					set.remove(normalizedAnchorText);
				}
			}
			
			for(String mention: set) {
				outputKey.set(mention);
				outputValue.set(0);
				output.collect(outputKey, outputValue);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, PairOfInts> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, PairOfInts> output, Reporter reporter) throws IOException {		
			int total = 0;
			int linked = 0;
			while (values.hasNext()) {
				++total;
				if (values.next().get() == 1) {
					++linked;
				}
			}	
			output.collect(key, new PairOfInts(linked, total));
		}
	}

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
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("anchor text index").create(ANCHOR_TEXT_INDEX_OPTION));

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
		String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, tmp), 
				cmdline.getOptionValue(ANCHOR_TEXT_INDEX_OPTION, DEFAULT_ANCHOR_TEXT_FILE),
				num_reducers
		);

		return 0;
	}
	
	@SuppressWarnings("deprecation")
	public void task1(Configuration config, String inputPath, String outputPath, 
			String anchorTextIndexPath, int num_reducers) throws IOException, URISyntaxException {
		LOG.info("Extracting keyphraseness index...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		LOG.info(" - anchor text index file: " + anchorTextIndexPath);
		LOG.info(" - number of reducers: " + num_reducers);

		JobConf conf = new JobConf(config, ExtractKeyphrasenessPipeline.class);
		conf.setJobName(String.format(
				"ExtractKeyphrasenessPipeline:[input: %s, output: %s, anchor text index file: %s]", 
				inputPath, 
				outputPath,
				anchorTextIndexPath
				)
		);
		conf.setJarByClass(ExtractKeyphrasenessPipeline.class);

		conf.setNumReduceTasks(num_reducers);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setMapperClass(ExtractKeyphrasenessPipeline.Map.class);
		conf.setReducerClass(ExtractKeyphrasenessPipeline.Reduce.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(anchorTextIndexPath + "#" + ANCHOR_TEXT_INDEX_SYMLINK), 
				conf);
		conf.set(ANCHOR_TEXT_INDEX_SYMLINK, ANCHOR_TEXT_INDEX_SYMLINK);
		
		JobClient.runJob(conf);		
	}
	
	public ExtractKeyphrasenessPipeline() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractKeyphrasenessPipeline(), args);
	}
}
