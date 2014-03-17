package knowledgebase;

import index.WikipediaRedirectPagesIndex;
import index.WikipediaTitlesIndex;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage.Link;
import edu.umd.cloud9.io.pair.PairOfIntString;

/*
 * Extracts anchor text - entity index and entity - entity index.
 * Entites are represented by wikipedia page ids. Debug version uses wikipedia page titles.
 * 1. Download and extract wikipedia xml dump from: 
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
 * 3. Use ExtractWikiArticleTitles to create a list of wikipedia article titles 
 * 		(see class for commands) and save the file in hadoop filesystem path: /enwiki-titles.txt
 * 4. Extract Wikipedia redirect pages mapping: (see https://code.google.com/p/wikipedia-redirect/)
 * 		to hadoop filesystem path: /enwiki-redirect.txt
 * 5. Create jar from project by using Eclipse: WikiPipeline.jar
 * 6. Run: 
 * 			hadoop WikiPipeline.jar knowledgebase.ExtractEntityMentionPipeline \
 * 				-input enwiki-latest.block -output wikipedia-index
 */
public class EntityMentionIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EntityMentionIndexBuilder.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };
  
	public static class Map extends MapReduceBase implements
			Mapper<IntWritable, WikipediaPage, PairOfIntString, IntWritable> {
		private static PairOfIntString outputKey = new PairOfIntString();
		private static IntWritable outputValue = new IntWritable();
		private static WikipediaRedirectPagesIndex redirectIndex;
		private static WikipediaTitlesIndex titlesIndex;

		@Override
		public void configure(JobConf job) {
			String redirectIndexPath = job.get(REDIRECT_SYMLINK);
      String titlesIndexPath = job.get(TITLES_SYMLINK);
			try {
				redirectIndex = WikipediaRedirectPagesIndex.load(redirectIndexPath);
  		  titlesIndex = WikipediaTitlesIndex.load(titlesIndexPath);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/*
		 *  Emit:	1. Mention-entity mapping: key = (1, anchor text), value = entity
		 *  			2. Entity-entity mapping:  key = (2, to entity), value = from entity
		 */
		@Override
		public void map(IntWritable key, WikipediaPage page, 
				OutputCollector<PairOfIntString, IntWritable> output, Reporter reporter) 
						throws IOException {	
			if (!page.isArticle()) {
				return;
			}
			reporter.incrCounter(Counters.PAGES_TOTAL, 1);
			String title = page.getTitle();
  	  int fromId = titlesIndex.getTitleId(title);
		
			for (Link link : page.extractLinks()) {	
				String normalizedAnchorText = Normalizer.processAnchorText(link.getAnchorText());
				String normalizedTarget = Normalizer.processTargetLink(link.getTarget());
				String canonicalTarget = WikiUtils.getCanonicalURL(normalizedTarget, redirectIndex);
  			int toId = titlesIndex.getTitleId(canonicalTarget);
				
				if (toId != -1 && !normalizedAnchorText.equals("")) {
					outputKey.set(1, normalizedAnchorText);
					outputValue.set(toId);
					output.collect(outputKey, outputValue);
				}

				if (toId != -1 && fromId != -1) {
					outputKey.set(2, String.valueOf(toId));
					outputValue.set(fromId);
					output.collect(outputKey, outputValue);
				}
			}
		
	/*		String normalizedContent = Normalizer.normalize(page.getContent());
			StringTokenizer tokenizer = new StringTokenizer(normalizedContent);
			Set<String> terms = new HashSet<String>();
			while (tokenizer.hasMoreTokens()) {
				terms.add(tokenizer.nextToken());
			}
			for (String term: terms) {
				outputKey.set(3, term);
				output.collect(outputKey, outputOne);
			}*/
		}
	}
	
	public static class Reduce extends MapReduceBase implements
	    Reducer<PairOfIntString, IntWritable, Text, Text> {
		private MultipleOutputs output;
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		@Override
		public void configure(final JobConf job) {
			super.configure(job);
			output = new MultipleOutputs(job);
		}

		@SuppressWarnings("unchecked")
		@Override
		public void reduce(PairOfIntString key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
			String outputFile = getOutputFile(key.getLeftElement());		
			HashSet<Integer> set = new HashSet<Integer>();	
			
			while (values.hasNext()) {
				set.add(values.next().get());
			}	
			
			outputKey.set(key.getRightElement());
			outputValue.set(StringUtils.join(set.toArray(), "\t"));
			output.getCollector(outputFile, reporter).collect(outputKey, outputValue);
		}


		public String getOutputFile(int key) {
			switch (key) {
			  case 1:
			  	return MENTION_INDEX;
			  case 2:
			  	return TO_ENTITY_INDEX;
			}
			throw new IllegalArgumentException("Key must be 1 or 2. Input key is " + key);
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
	
	private static final int DEFAULT_NUM_REDUCERS = 10;
	private static final String DEFAULT_TITLES_INDEX_FILE = "/enwiki-titles.txt";
	private static final String DEFAULT_REDIRECT_FILE = "/enwiki-redirect.txt";
	
	private static final String TITLES_SYMLINK = "titles_file";
	private static final String REDIRECT_SYMLINK = "redirect_file";
	static final String MENTION_INDEX = "mention";
	static final String TO_ENTITY_INDEX = "to";

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
		String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION, tmp), 
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

		JobConf conf = new JobConf(config, EntityMentionIndexBuilder.class);
		conf.setJobName(String.format(
				"EntityMentionIndexBuilder:[input: %s, output: %s, titles: %s, redirect mapping: %s]", 
				inputPath, 
				outputPath,
				titlesPath,
				redirectMapPath
				)
		);
		conf.setJarByClass(EntityMentionIndexBuilder.class);

		conf.setNumReduceTasks(num_reducers);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		MultipleOutputs.addNamedOutput(conf, MENTION_INDEX, TextOutputFormat.class, Text.class, 
				Text.class);
		MultipleOutputs.addNamedOutput(conf, TO_ENTITY_INDEX, TextOutputFormat.class, Text.class, 
				Text.class);
		
		conf.setMapOutputKeyClass(PairOfIntString.class);
		conf.setMapOutputValueClass(IntWritable.class);
//	conf.setCompressMapOutput(true);

		conf.setMapperClass(EntityMentionIndexBuilder.Map.class);
		conf.setReducerClass(EntityMentionIndexBuilder.Reduce.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(redirectMapPath + "#" + REDIRECT_SYMLINK), conf);
	  DistributedCache.addCacheFile(new URI(titlesPath + "#" + TITLES_SYMLINK), conf);
		conf.set(TITLES_SYMLINK, TITLES_SYMLINK);
		conf.set(REDIRECT_SYMLINK, REDIRECT_SYMLINK);
		
		JobClient.runJob(conf);
	}
	
	public EntityMentionIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new EntityMentionIndexBuilder(), args);
	}
}