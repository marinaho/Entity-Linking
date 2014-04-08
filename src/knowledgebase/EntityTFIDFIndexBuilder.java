package knowledgebase;

import java.io.IOException;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;

public class EntityTFIDFIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EntityTFIDFIndexBuilder.class);
  private static final Random RANDOM = new Random();
	
  private static enum Blocks {
    Total
  };

  public static class Map extends MapReduceBase 
  		implements MapRunnable<LongWritable, Text, LongWritable, Text> {
  	private static LongWritable key = new LongWritable();
  	private static Text inputValue = new Text();
  	private static Text outputValue = new Text();
  	private static int fileno;

  	public void configure(JobConf job) {
      String file = job.get("map.input.file");
      fileno = Integer.parseInt(file.substring(file.indexOf("part-") + 5));
    }

  	@Override
  	public void run(RecordReader<LongWritable, Text> input,
        OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
  		long offset = input.getPos();
  		while (input.next(key, inputValue)) {
  			outputValue.set(offset + "\t" + fileno);
  			output.collect(key, outputValue);
  		
  			reporter.incrCounter(Blocks.Total, 1);	
  			offset = input.getPos();
  		}
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
		LOG.info("Extracting TF-IDF vectors for wiki articles...");
		LOG.info(" - input: " + inputPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(config, EntityTFIDFIndexBuilder.class);
    FileSystem fs = FileSystem.get(conf);
    
		conf.setJobName(String.format(
				"EntityTFIDFIndexBuilder:[input: %s, output: %s]", 
				inputPath, 
				outputPath
				)
		);
		conf.setJarByClass(EntityTFIDFIndexBuilder.class);

    String tmpPath = "tmp-" + this.getClass().getSimpleName() + "-"
        + RANDOM.nextInt(10000);
    
		SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
		TextOutputFormat.setOutputPath(conf, new Path(tmpPath));
		
		conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setMapRunnerClass(Map.class);
		conf.setReducerClass(IdentityReducer.class);
		
		conf.setNumReduceTasks(1);

		// Delete the output directory if it exists already.
		fs.delete(new Path(tmpPath), true);
		
		RunningJob job = JobClient.runJob(conf);
		
    int blocks = (int) job.getCounters().getCounter(Blocks.Total);

    LOG.info("number of blocks: " + blocks);

    LOG.info("Writing index file...");
    LineReader reader = new LineReader(fs.open(new Path(tmpPath + "/part-00000")));
    FSDataOutputStream out = fs.create(new Path(outputPath), true);

    out.writeUTF(EntityTFIDFIndexBuilder.class.getCanonicalName());
    out.writeUTF(inputPath.toString());
    out.writeInt(blocks);

    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\t");

      int docno = Integer.parseInt(arr[0]);
      int offset = Integer.parseInt(arr[1]);
      short fileno = Short.parseShort(arr[2]);

      out.writeInt(docno);
      out.writeInt(offset);
      out.writeShort(fileno);

      cnt++;

      if (cnt % 1000000 == 0) {
        LOG.info(cnt + " blocks written");
      }
    }

    reader.close();
    out.close();

    if (cnt != blocks) {
      throw new RuntimeException("Error: mismatch in block count!");
    }

    // Clean up.
    fs.delete(new Path(tmpPath), true);
	}
	
	public EntityTFIDFIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new EntityTFIDFIndexBuilder(), args);
	}
}
