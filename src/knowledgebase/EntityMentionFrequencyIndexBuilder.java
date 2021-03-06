package knowledgebase;

import index.RedirectPagesIndex;
import index.TitlesIndex;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Random;

import normalizer.Normalizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import data.CandidatesEntry;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII.Entry;

/**
 * Extracts for each name a list of candidate entities and their frequencies. Only the top most
 * frequent candidates are kept. Cutoff point is determined by LIMIT_CANDIDATES.
 * Extracts anchor text - entity index and entity - entity index.
 * The anchor text - entity index can be obtained by merging the files that start with 
 * the string contained in MENTION_INDEX.
 * The entity - inlinks count index can be obtained by merging the files that start with 
 * the string contained in ENTITY_ENTITY_INDEX.
 * These indices can be loaded into memory by using @See AnchorTextIndex and @See EntityEntityIndex.
 * 
 * Entities are represented by Wikipedia page ids. Debug version outputs Wikipedia page titles.
 * 1. Download and extract Wikipedia xml dump from: 
 * 		http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
 * 2. Repack wikipedia using Cloud9 into sequence file format: 
 * 		(See http://lintool.github.io/Cloud9/docs/content/wikipedia.html)
 * a) Create sequentially-numbered docnos: 
 * 			hadoop edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder \
 *				-input enwiki-latest-pages-articles.xml -output_file enwiki-latest-docno.dat 
 *				-wiki_language en -keep_all 
 * b) Repack wikipedia using block compression:
 * 			hadoop edu.umd.cloud9.collection.wikipedia.RepackWikipedia \
 * 				-input enwiki-latest-pages-articles.xml \
 * 				-output enwiki-latest.block -wiki_language en \
 * 				-mapping_file enwiki-latest-docno.dat -compression_type block
 * 3. Use WikiArticleTitlesIndexBuilder to create a list of wikipedia article titles 
 * 		(see class for commands) and save the file in hadoop filesystem path: /enwiki-titles.txt
 * 4. Extract Wikipedia redirect pages mapping: (see https://code.google.com/p/wikipedia-redirect/)
 * 		Preprocess the mapping with: PreprocessRedirectMapping and store result in hadoop filesystem
 * 		to /enwiki-redirect.txt
 * 5. Create jar from project by using Eclipse: WikiPipeline.jar
 * 6. Run: 
 * 			hadoop WikiPipeline.jar knowledgebase.ExtractEntityMentionIndexBuilder \
 * 				-input enwiki-latest.block -output /wikipedia-index
 */
public class EntityMentionFrequencyIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EntityMentionFrequencyIndexBuilder.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };
  
	public static class Map extends MapReduceBase 
			implements Mapper<IntWritable, WikipediaPage, Text, IntWritable> {
		private static final Text outputKey = new Text();
		private static final IntWritable outputValue = new IntWritable();
		private static RedirectPagesIndex redirectIndex;
		private static TitlesIndex titlesIndex;

		@Override
		public void configure(JobConf job) {
			BasicConfigurator.configure();
			String redirectIndexPath = job.get(REDIRECT_SYMLINK);
      String titlesIndexPath = job.get(TITLES_SYMLINK);
			try {
				redirectIndex = RedirectPagesIndex.load(redirectIndexPath);
  		  titlesIndex = TitlesIndex.load(titlesIndexPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 *  Emits: 1. Mention-entity mapping: key = anchor text, value = entity
		 *  Doesn't emit duplicate links from same page.
		 */
		@Override
		public void map(IntWritable key, WikipediaPage page, OutputCollector<Text, IntWritable> output, 
				Reporter reporter) throws IOException {	
			if (!page.isArticle()) {
				return;
			}
			reporter.incrCounter(Counters.PAGES_TOTAL, 1);			

			for (Link link : page.extractLinks()) {	
				String normalizedAnchorText = Normalizer.processAnchorText(link.getAnchorText());
				String normalizedTarget = Normalizer.processTargetLink(link.getTarget());
				String canonicalTarget = redirectIndex.getCanonicalURL(normalizedTarget);
  			int targetEntityId = titlesIndex.getTitleId(canonicalTarget);
  			
  			if (targetEntityId != TitlesIndex.NOT_CANONICAL_TITLE && 
  					StringUtils.isNotBlank(normalizedAnchorText)) {					
  				outputKey.set(normalizedAnchorText);
  				outputValue.set(targetEntityId);
  				output.collect(outputKey, outputValue);
  			}
			}
		}
	}
	 
	public static class Reduce extends MapReduceBase 
			implements Reducer<Text, IntWritable, Text, CandidatesEntry> {
		private static double LIMIT_CANDIDATES = 0.02;
		
		/**
		 * Emits: 1. (key = anchor text, value = top ranked entities and their frequency counts
		 */
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, CandidatesEntry> output, Reporter reporter) throws IOException {
			int total = 0;
			HMapII frequencyMap = new HMapII();
			while (values.hasNext()) {
				frequencyMap.increment(values.next().get());
				++total;
			}
			CandidatesEntry nameEntry = new CandidatesEntry(total);
			for (Entry entry: frequencyMap.getEntriesSortedByValue()) {
				int entity = entry.getKey();
				int frequency = entry.getValue();
				if (frequency < total * LIMIT_CANDIDATES) {
					break;
				}
				nameEntry.setCandidateFrequency(entity, frequency);
			}
			if (nameEntry.getCandidatesCount() > 0) {
				output.collect(key, nameEntry);
			}
		}
	}
	
	private static final String INPUT_OPTION = "input";
	private static final String NUM_REDUCERS_OPTION = "num_reducers";
	private static final String OUTPUT_OPTION = "output";
	private static final String TITLES_INDEX_OPTION = "titles";
	// File containing pairs of (redirect title, main title).
	// Obtained using edu.cmu.lti.wikipedia_redirect.WikipediaRedirectExtractor .
	// See https://code.google.com/p/wikipedia-redirect/ .
	private static final String REDIRECT_MAPPING_OPTION = "redirect_map";
	
	private static final int DEFAULT_NUM_REDUCERS = 1;
	private static final String DEFAULT_TITLES_INDEX_FILE = "/enwiki-titles.txt";
	private static final String DEFAULT_REDIRECT_FILE = "/enwiki-redirect.txt";
	
	private static final String TITLES_SYMLINK = "titles_file";
	private static final String REDIRECT_SYMLINK = "redirect_file";
	
	static final String MENTION_INDEX = "mention";
	static final String ENTITY_ENTITY_INDEX = "to";

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
				.hasArg().withDescription("titles index").create(TITLES_INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path")
				.hasArg().withDescription("redirect mapping").create(REDIRECT_MAPPING_OPTION));

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
		String defaultOutput = "output-" + this.getClass().getCanonicalName() + "-" + 
				random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, defaultOutput), 
				cmdline.getOptionValue(TITLES_INDEX_OPTION, DEFAULT_TITLES_INDEX_FILE),
				cmdline.getOptionValue(REDIRECT_MAPPING_OPTION, DEFAULT_REDIRECT_FILE),
				num_reducers
		);

		return 0;
	}
	
	@SuppressWarnings("deprecation")
	public void task1(Configuration config, String inputPath, String outputPath, String titlesPath,
			String redirectMapPath, int num_reducers) throws IOException, URISyntaxException {
		LOG.info("Extracting mention-entity & entity-entity mapping...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);
		LOG.info(" - titles index: " + titlesPath);
		LOG.info(" - redirect mapping file: " + redirectMapPath);
		LOG.info(" - number of reducers: " + num_reducers);

		JobConf conf = new JobConf(config, EntityMentionFrequencyIndexBuilder.class);
		conf.setJobName(String.format(
				"EntityMentionFrequencyIndexBuilder:[input: %s, output: %s, titles: %s, "
				+ "redirect mapping: %s]", 
				inputPath, 
				outputPath,
				titlesPath,
				redirectMapPath
				)
		);
		conf.setJarByClass(EntityMentionFrequencyIndexBuilder.class);

		conf.setNumReduceTasks(num_reducers);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(CandidatesEntry.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(redirectMapPath + "#" + REDIRECT_SYMLINK), conf);
	  DistributedCache.addCacheFile(new URI(titlesPath + "#" + TITLES_SYMLINK), conf);
		conf.set(TITLES_SYMLINK, TITLES_SYMLINK);
		conf.set(REDIRECT_SYMLINK, REDIRECT_SYMLINK);
		
		JobClient.runJob(conf);
	}
	
	public EntityMentionFrequencyIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new EntityMentionFrequencyIndexBuilder(), args);
	}
}
