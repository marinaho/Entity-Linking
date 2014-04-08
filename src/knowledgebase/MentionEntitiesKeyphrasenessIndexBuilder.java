package knowledgebase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfIntString;

/*
 * Puts together in one file: mention \t keyphraseness \t candidate entities.
 */
public class MentionEntitiesKeyphrasenessIndexBuilder extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(EntityMentionIndexBuilder.class);

	private static final String INPUT_ENTITIES_OPTION = "input_entities";
	private static final String INPUT_KEYPHRASENESS_OPTION = "input_keyphraseness";
	private static final String OUTPUT_OPTION = "output";
	
	public static class KeyphrasenessMapper extends MapReduceBase implements 
			Mapper<LongWritable, Text, Text, PairOfIntString> {
		Text outputKey = new Text();
		
		// Emit: key = mention; value = (1, candidate entities)
		public void map(LongWritable key, Text value, OutputCollector<Text, PairOfIntString> output, 
				Reporter reporter) throws IOException {
			String[] parts = value.toString().split("\t", 2);
			outputKey.set(parts[0]);
			output.collect(outputKey, new PairOfIntString(1, parts[1]));
		}
	}
	
	public static class CandidateEntitiesMapper extends MapReduceBase implements 
			Mapper<LongWritable, Text, Text, PairOfIntString> {
		Text outputKey = new Text();

		// Emit: key = mention; value = (2, candidate entities)
		public void map(LongWritable key, Text value, OutputCollector<Text, PairOfIntString> output, 
				Reporter reporter) throws IOException {
			String[] parts = value.toString().split("\t", 2);
			outputKey.set(parts[0]);
			output.collect(outputKey, new PairOfIntString(2, parts[1]));
		}
	}
	
	public static class Reduce extends MapReduceBase implements 
			Reducer<Text, PairOfIntString, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<PairOfIntString> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String candidates = "", keyphraseness = "";
			while(values.hasNext()) {
				PairOfIntString pair = values.next();
				if (pair.getLeftElement() == 1) {
					keyphraseness = pair.getRightElement();
				} else {
					candidates = pair.getRightElement();
				}
			}
			
			if (!candidates.equals("") && !keyphraseness.equals("")) {
				Pattern pattern = Pattern.compile("\\((\\d+),\\s+(\\d+)");
				Matcher matcher = pattern.matcher(keyphraseness);
				matcher.find();
				int linked = Integer.parseInt(matcher.group(1));
				int total = Integer.parseInt(matcher.group(2));
				output.collect(key, new Text(linked + "\t" + total + "\t" + candidates));
			}
		}
	}
	
	@SuppressWarnings("static-access")
	public int run(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input candidate entities").isRequired().create(INPUT_ENTITIES_OPTION));
		options.addOption(OptionBuilder.withArgName("num_reducers").hasArg()
				.withDescription("input keyphraseness").isRequired().create(INPUT_KEYPHRASENESS_OPTION));
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

		task1(
				getConf(),
				cmdline.getOptionValue(INPUT_ENTITIES_OPTION), 
				cmdline.getOptionValue(INPUT_KEYPHRASENESS_OPTION), 
				cmdline.getOptionValue(OUTPUT_OPTION)
		);

		return 0;
	}
	
	public void task1(Configuration config, String inputEntitiesPath, String inputKeyphrasenessPath, 
			String outputPath) throws IOException, URISyntaxException {
		LOG.info("Extracting mention-keyphraseness-candidate entities mapping...");
		LOG.info(" - input entities: " + inputEntitiesPath);
		LOG.info(" - input keyphraseness: " + inputKeyphrasenessPath);
		LOG.info(" - output: " + outputPath);

		JobConf conf = new JobConf(config, MentionEntitiesKeyphrasenessIndexBuilder.class);
		conf.setJobName(String.format(
				"MentionEntitiesKeyphrasenessIndexBuilder:[input entities: %s, input keyphraseness: %s,"
				+ " output: %s]", 
				inputEntitiesPath, 
				inputKeyphrasenessPath,
				outputPath
				)
		);
		conf.setJarByClass(MentionEntitiesKeyphrasenessIndexBuilder.class);

		conf.setNumReduceTasks(1);

		MultipleInputs.addInputPath(conf, new Path(inputEntitiesPath), 
				TextInputFormat.class, CandidateEntitiesMapper.class);
		MultipleInputs.addInputPath(conf, new Path(inputKeyphrasenessPath),
				TextInputFormat.class, KeyphrasenessMapper.class);
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(PairOfIntString.class);
	
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// Delete the output directory if it exists already.
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);
	}
	
	public MentionEntitiesKeyphrasenessIndexBuilder() {
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MentionEntitiesKeyphrasenessIndexBuilder(), args);
	}

}
