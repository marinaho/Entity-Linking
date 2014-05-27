package baseline;

import iitb.Annotation;
import iitb.IITBDataset;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import md.MentionDetection;

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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
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
import org.xml.sax.SAXException;

import evaluation.Verifier;

/*
 * Shards testing documents and runs Entity Linking using Random Graph Walk method.
 * Not tested/used yet. Seems that distributed caching takes a long time to manage all indices.
 */
public class BaselinePipeline extends Configured implements Tool {
private static final Logger LOG = Logger.getLogger(BaselinePipeline.class);
	
  private static enum Counters {
    PAGES_TOTAL
  };

  public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

  	private static MentionIndex mentionIndex;
  	private static EntityLinksIndex entityLinksIndex;
  	private static EntityTFIDFIndex entityTFIDFIndex;
  	private static TermDocumentFrequencyIndex dfIndex;
  	private static IITBDataset iitb;
  	private static String testFilesPath;
  	
		@Override
		public void configure(JobConf job) {
			String mentionIndexPath = job.get(MENTION_FILE_SYMLINK);
			String entityLinksIndexPath = job.get(ENTITY_LINKS_FILE_SYMLINK);
			String tfidfEntitiesIndexPath = job.get(TFIDF_ENTITIES_FILE_SYMLINK);
			String dfTermIndexPath = job.get(DF_TERM_FILE_SYMLINK);
			String annotationsFilePath = job.get(ANNOTATIONS_FILE_SYMLINK);
			String titlesIndexPath = job.get(TITLES_FILE_SYMLINK);
			String redirectsIndexPath = job.get(REDIRECTS_FILE_SYMLINK);
			testFilesPath = job.get(TEST_FILES_PATH);
			
			try {
				mentionIndex = MentionIndex.load(mentionIndexPath);
				entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
				entityTFIDFIndex = new EntityTFIDFIndex(new Configuration(), null);
				entityTFIDFIndex.load(new Path(tfidfEntitiesIndexPath));
				dfIndex = TermDocumentFrequencyIndex.load(dfTermIndexPath);
				iitb = new IITBDataset(titlesIndexPath, redirectsIndexPath);
				iitb.load(annotationsFilePath, testFilesPath, true);
			} catch (IOException e1) {
				e1.printStackTrace();
			} catch (ParserConfigurationException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			}
		}
		
  	/*
  	 * Runs Entity Linking on a test file by using Random Graph Walk method. 
  	 */
  	@Override
  	public void map(Text filename, Text content, 
  			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
  		reporter.getCounter(Counters.PAGES_TOTAL).increment(1);
  			
  		MentionDetection md = new MentionDetection(content.toString(), mentionIndex, entityTFIDFIndex, 
  				dfIndex);
  		RandomGraphWalk baseline = new RandomGraphWalk(entityLinksIndex);
  		baseline.solve(md.solve());
  		Set<Annotation> solution = baseline.getSolutionAnnotations(filename.toString());
  		Verifier<Annotation> verifier = new Verifier<Annotation>();
  		verifier.computeResults(solution, iitb.getAnnotations(filename.toString()));
  		output.collect(filename, new Text(verifier.toString()));
  	}
  }

  // Directory of test files
	private static final String INPUT_OPTION = "inputDir";
	private static final String OUTPUT_OPTION = "output";
	// Location of ground truth annotations
	private static final String ANNOTATIONS_OPTION = "annotations";
	// Location of mention dictionary file. @See  index.MentionIndex
	private static final String MENTION_OPTION = "mentions";
	// Location of entity links file. @See index.EntityLinksIndex
	private static final String ENTITY_LINK_OPTION = "entity_links";
	// Location of tf-idf of entities index. @See index.EntityTFIDFIndex
	private static final String TFIDF_ENTITIES_OPTION = "tfidf_entities_index";
	// Location of tf-idf entities files of the index.
	private static final String TFIDF_ENTITIES_PATH_OPTION = "tfidf_entities_path";
	// Location of term document frequency index. @See index.TermDocumentFrequencyIndex
	private static final String DF_TERM_OPTION = "df";
	// Location of wiki titles file
	private static final String TITLES_OPTION = "titles";
	// Location of wiki redirects file
	private static final String REDIRECTS_OPTION = "redirect";
	
	// Default location of ground truth annotations, used when ANNOTATIONS_OPTION is not specified.
	private static final String DEFAULT_ANNOTATIONS_FILE = "/CSAW_Annotations.xml";
	private static final String DEFAUL_ANCHOR_TEXT_FILE = "/mention-entity-index.txt";
	private static final String DEFAULT_ENTITY_LINKS_FILE = "/entity-entity-index.txt";
	private static final String DEFAULT_TFIDF_ENTITIES_FILE = "/tf-idf-entity-index.txt";
	private static final String DEFAULT_DF_TERM_FILE = "/df-index.txt";
	private static final String DEFAULT_TFIDF_ENTITIES_PATH = "tf-idf-entity";
	private static final String DEFAULT_TITLES_PATH = "/enwiki-titles.txt";
	private static final String DEFAULT_REDIRECTS_PATH = "/enwiki-redirect.txt";
	
	// Symlinks used by the distributed cache
	private static final String MENTION_FILE_SYMLINK = "m";
	private static final String ENTITY_LINKS_FILE_SYMLINK = "e";
	private static final String TFIDF_ENTITIES_FILE_SYMLINK = "t";
	private static final String DF_TERM_FILE_SYMLINK = "d";
	private static final String ANNOTATIONS_FILE_SYMLINK = "a";
	private static final String TITLES_FILE_SYMLINK = "tt";
	private static final String REDIRECTS_FILE_SYMLINK = "r";
	
	private static final String TEST_FILES_PATH = "/crawledDocs/";
	
	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("directory with test documents").isRequired().create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("annotations file").create(ANNOTATIONS_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription(
				"mention to keyphraseness and candidate entities index file").create(MENTION_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index of entity links file").create(ENTITY_LINK_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index of tfidf of entities file").create(TFIDF_ENTITIES_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index of document frequency for terms").create(DF_TERM_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index of wiki canonical titles").create(TITLES_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index of wiki redirects").create(REDIRECTS_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output").create(OUTPUT_OPTION));

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
		String tmp = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000) + ".seq";
		
		String titlesPath = cmdline.getOptionValue(TITLES_OPTION, DEFAULT_TITLES_PATH);
		String redirectsPath = cmdline.getOptionValue(REDIRECTS_OPTION, DEFAULT_REDIRECTS_PATH);
		String annotationsFile = cmdline.getOptionValue(ANNOTATIONS_OPTION, DEFAULT_ANNOTATIONS_FILE);
		String mentionIndexFile = cmdline.getOptionValue(MENTION_OPTION, DEFAUL_ANCHOR_TEXT_FILE);
		String termDFFile = cmdline.getOptionValue(DF_TERM_OPTION, DEFAULT_DF_TERM_FILE);
		String entityLinksIndexFile = 
				cmdline.getOptionValue(
						ENTITY_LINK_OPTION, 
						DEFAULT_ENTITY_LINKS_FILE
				);
		String tfidfEntitiesFile = 
				cmdline.getOptionValue(
						TFIDF_ENTITIES_OPTION, 
						DEFAULT_TFIDF_ENTITIES_FILE
		);
		String tfidfEntitiesPath = 
				cmdline.getOptionValue(
						TFIDF_ENTITIES_PATH_OPTION, 
						DEFAULT_TFIDF_ENTITIES_PATH
				);
		
		String output = "tmp-" + this.getClass().getCanonicalName() + "-" + random.nextInt(10000);
		
		createSequenceFile(getConf(), cmdline.getOptionValue(INPUT_OPTION), tmp);
		task1(
				getConf(),
				titlesPath,
				redirectsPath,
				tfidfEntitiesPath,
				tmp, 
				mentionIndexFile,
				entityLinksIndexFile,
				tfidfEntitiesFile,
				termDFFile,
				annotationsFile,
				cmdline.getOptionValue(OUTPUT_OPTION, output)
		);
		
		// Clean-up
		FileSystem.get(getConf()).delete(new Path(tmp), true);

		return 0;
	}
	
	/*
	 * Creates sequence file with key = filename, value = content from the testing files.
	 */
	public void createSequenceFile(Configuration conf, String testFilesPath, 
			String sequenceFilePath) throws IOException {
		LOG.info("Creating sequence file: " + sequenceFilePath);
	  Path seqFilePath = new Path(sequenceFilePath);
	  SequenceFile.Writer writer = SequenceFile.createWriter(
	  		conf, 
	  		Writer.file(seqFilePath), 
	  		Writer.keyClass(Text.class),
	      Writer.valueClass(Text.class)
	  );

	  FileSystem fs = FileSystem.get(conf);
	  Text key = new Text();
	  Text value = new Text();
	  for (FileStatus file: fs.listStatus(new Path(testFilesPath))) {
	  	if (!file.isDirectory()) {
	  		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
	  		String line;
	  		StringBuilder content = new StringBuilder("");
        while ((line=br.readLine()) != null) {
        	content.append(" " + line);
        }
        
        key.set(file.getPath().getName());
        value.set(content.toString());
        writer.append(key, value);
	  	}
	  }
    writer.close();
	}
	
	@SuppressWarnings("deprecation")
	public void task1(
			Configuration config, 
			String titlesPath,
			String redirectsPath,
			String entityTFIDFDirectoryPath, // Directory containing entity tf-idf index files. 
			String inputFile, // File created by the createSequenceFile()
			String mentionIndexFile, 
			String entityLinksIndexFile,
			String tfidfEntitiesFile, 
			String termDFFile, 
			String annotationsFile, 
			String outputPath) 
					throws IOException, URISyntaxException {
		LOG.info("Entity linking using Random Graph Walk...");
		LOG.info(" - input: " + inputFile);
		LOG.info(" - output: " + outputPath);


		JobConf conf = new JobConf(config, BaselinePipeline.class);
		conf.setJobName(String.format(
				"BaselinePipeline:[input: %s, output: %s]", 
				inputFile, 
				outputPath
				)
		);
		conf.setJarByClass(BaselinePipeline.class);

		SequenceFileInputFormat.addInputPath(conf, new Path(inputFile));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);

		conf.setNumReduceTasks(0);
		
		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(titlesPath + "#" + TITLES_FILE_SYMLINK), conf);
		DistributedCache.addCacheFile(new URI(redirectsPath + "#" + REDIRECTS_FILE_SYMLINK), conf);
		DistributedCache.addCacheFile(new URI(mentionIndexFile + "#" + MENTION_FILE_SYMLINK), conf);
		DistributedCache.addCacheFile(new URI(termDFFile + "#" + DF_TERM_FILE_SYMLINK), conf);
		DistributedCache.addCacheFile(new URI(annotationsFile + "#" + ANNOTATIONS_FILE_SYMLINK), conf);
	  DistributedCache.addCacheFile(
	  		new URI(entityLinksIndexFile + "#" + ENTITY_LINKS_FILE_SYMLINK), 
	  		conf
	  );
		DistributedCache.addCacheFile(
				new URI(tfidfEntitiesFile + "#" + TFIDF_ENTITIES_FILE_SYMLINK), 
				conf
		);
		FileSystem fs = FileSystem.get(conf);
	  for (FileStatus file: fs.listStatus(new Path(entityTFIDFDirectoryPath))) {
	  	if (!file.isDirectory()) {
	  		String filename = file.getPath().getName();
	  		String filepath = file.getPath().toString();
	  		DistributedCache.addCacheFile(new URI(filepath + '#' + filename), conf);
	  	}
	  }
	  
		conf.set(MENTION_FILE_SYMLINK, MENTION_FILE_SYMLINK);
		conf.set(ENTITY_LINKS_FILE_SYMLINK, ENTITY_LINKS_FILE_SYMLINK);
		conf.set(TFIDF_ENTITIES_FILE_SYMLINK, TFIDF_ENTITIES_FILE_SYMLINK);
		conf.set(DF_TERM_FILE_SYMLINK, DF_TERM_FILE_SYMLINK);
		conf.set(ANNOTATIONS_FILE_SYMLINK, ANNOTATIONS_FILE_SYMLINK);
		conf.set(TITLES_FILE_SYMLINK, TITLES_FILE_SYMLINK);
		conf.set(REDIRECTS_FILE_SYMLINK, REDIRECTS_FILE_SYMLINK);
		conf.set(TEST_FILES_PATH, TEST_FILES_PATH);
		
		JobClient.runJob(conf);
	}
	
	public BaselinePipeline() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new BaselinePipeline(), args);
	}
}