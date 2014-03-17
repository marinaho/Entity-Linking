package knowledgebase;

import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * TO DO: add description
 *
 * @author Marina Horlescu
 */
public class WikiPipeline extends Configured implements Tool {
	
	public static enum PageTypes {
		TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
	};

	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	private static final String NUM_REDUCERS_OPTION = "num_reducers";
	// File containing pairs of (redirect title, main title).
	// Obtained using edu.cmu.lti.wikipedia_redirect.WikipediaRedirectExtractor .
	// See https://code.google.com/p/wikipedia-redirect/ .
	private static final String REDIRECT_MAPPING_OPTION = "redirect_map";
	// Provide anchor text index file or it will be computed on the spot (computationally expensive).
	private static final String ANCHOR_TEXT_INDEX_OPTION = "anchor_text";
	private static final String TITLES_INDEX_OPTION = "titles";

	private static final int DEFAULT_NUM_REDUCERS = 10;
	private static final String DEFAULT_TITLES_INDEX_FILE = "/enwiki-titles.txt";
	private static final String DEFAULT_REDIRECT_FILE = "/enwiki-redirect.txt";
	private static String DEFAULT_ANCHOR_TEXT_FILE = "mention_entity_index";

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

		if (!cmdline.hasOption(ANCHOR_TEXT_INDEX_OPTION)) {
				new EntityMentionIndexBuilder()
						.task1(
								getConf(),
								cmdline.getOptionValue(INPUT_OPTION), 
								DEFAULT_ANCHOR_TEXT_FILE, 
								cmdline.getOptionValue(TITLES_INDEX_OPTION, DEFAULT_TITLES_INDEX_FILE),
								cmdline.getOptionValue(REDIRECT_MAPPING_OPTION, DEFAULT_REDIRECT_FILE),
								1
						);
				DEFAULT_ANCHOR_TEXT_FILE += "/" + EntityMentionIndexBuilder.MENTION_INDEX + "-r-00000";
		}

		new KeyphrasenessIndexBuilder()
				.task1(
						getConf(),
						cmdline.getOptionValue(INPUT_OPTION), 
						cmdline.getOptionValue(OUTPUT_OPTION, tmp), 
						cmdline.getOptionValue(ANCHOR_TEXT_INDEX_OPTION, DEFAULT_ANCHOR_TEXT_FILE),
						num_reducers
				);
		return 0;
	}
	
	public WikiPipeline() {
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new WikiPipeline(), args);
	}
}
