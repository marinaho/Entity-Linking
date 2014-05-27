package tests;

import index.EntityTFIDFIndex;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestSequenceFileReader extends Configured implements Tool {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  	private static String filePath;
  	private static Configuration conf;
  	
		@Override
		public void configure(JobConf job) {
			filePath = job.get(FILE_SYMLINK);
      conf = new Configuration();
		}
		
  	/*
  	 * Runs Entity Linking on a test file by using Random Graph Walk method. 
  	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, 
  	 * org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
  	 */
  	@Override
  	public void map(LongWritable filename, Text content, 
  			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
  //		File f = new File(filePath);
  		EntityTFIDFIndex index = new EntityTFIDFIndex(conf, null);
  		index.load(new Path(filePath));
  		output.collect(new Text(index.getCollectionPath()), new Text(index.getCollectionPath()));
  	}
  }

  // Directory of test files
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	
	// Default location of input, used when INPUT_OPTION is not specified.
	private static final String DEFAULT_INPUT_FILE = "/tf-df-entity-index.txt";
	private static final String FILE_SYMLINK = "file_symlink";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("directory with test documents").create(INPUT_OPTION));
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

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_OPTION, DEFAULT_INPUT_FILE),
				cmdline.getOptionValue(OUTPUT_OPTION, tmp)
		);

		return 0;
	}
	
	@SuppressWarnings("deprecation")
	public void task1(Configuration config, String inputFile, String outputPath) 
					throws IOException, URISyntaxException {

		JobConf conf = new JobConf(config, TestSequenceFileReader.class);
		conf.setJobName(String.format(
				"TestSequenceFileReader:[input: %s, output: %s]", 
				inputFile, 
				outputPath
				)
		);
		conf.setJarByClass(TestSequenceFileReader.class);

		TextInputFormat.addInputPath(conf, new Path("/enwiki-sample-docno.dat"));
		TextOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);

		conf.setNumReduceTasks(0);
		
		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(inputFile + "#" + FILE_SYMLINK), conf);
		conf.set(FILE_SYMLINK, FILE_SYMLINK);
		
// 		EntityTFIDFIndex index = new EntityTFIDFIndex(conf);
//		index.load(new Path(inputFile));
		
		JobClient.runJob(conf);
	}
	
	public TestSequenceFileReader() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TestSequenceFileReader(), args);
	}

}