package tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import iitb.Annotation;
import iitb.IITBDataset;
import index.AnchorTextIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.RedirectPagesIndex;
import index.TermDocumentFrequencyIndex;
import index.TitlesIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import knowledgebase.KeyphrasenessIndexBuilder;
import md.Mention;
import md.MentionDetection;
import md.Ngram;
import md.Token;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;

public class UnitTests {
	private static final int NGRAM_SIZE = 3;
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations.xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";

	public static void main(String args[]) throws Exception {
//		testExtractContext();
//		testAnnotationsParsing();
//		testGatherNgrams();
//		printMemoryRequirements();
//		aux();
		testMentionDetection();
//		testMentionDetection2();
//		testAnnotationsParsing();
	}

	public static void aux()  throws IOException {
		String text = IITBDataset.getFileContent("/home/marinah/input/crawledDocs/13Oct08AmitTech14.txt");
		MentionDetection md = new MentionDetection(text, null, null, null);
		List<Token> tokens = MentionDetection.tokenizeText(text);
		List<Ngram> ngrams = md.gatherNgrams(tokens, NGRAM_SIZE);
		int i = 0;
		for (Ngram ngram: ngrams) {
			++i;
			if (i < 3000)
			System.out.println(ngram);
		}
	}
	
	public static void testExtractContext() {
		Mention.WINDOW_SIZE = 3;
		Mention mention1 = new Mention("los", 17, 3);
		Mention mention2 = new Mention("in", 14, 2);
		Mention mention3 = new Mention("lived", 8, 5);
		Mention mention4 = new Mention("michael",0, 5);
		List<Token> tokens = Arrays.asList(
				new Token("michael", 0), 
				new Token("lived", 8), 
				new Token("in", 14),
				new Token("los", 17)
		);
		assertTrue(CollectionUtils.isEqualCollection(
				mention1.extractContext(tokens), 
				Arrays.asList("lived", "in", "los")));
		assertTrue(CollectionUtils.isEqualCollection(
				mention2.extractContext(tokens), 
				Arrays.asList("lived", "in", "los")));
		assertTrue(CollectionUtils.isEqualCollection(
				mention3.extractContext(tokens), 
				Arrays.asList("michael", "lived", "in")));
		assertTrue(CollectionUtils.isEqualCollection(
				mention4.extractContext(tokens), 
				Arrays.asList("michael", "lived", "in")));
		System.out.println("=== testExtractContext passed ===");
	}
	
	public static void testMentionDetection() throws IOException {
		String text = "our bodies use carbohydrates. glycogen, amino-acids ";
		MentionDetection md = new MentionDetection(text, null, null, null);
		List<Token> tokens = MentionDetection.tokenizeText(text);
		List<Token> expected = Arrays.asList(
					new Token("our", 0),
					new Token("bodies", 4),
					new Token("use", 11),
					new Token("carbohydrates", 15),
					new Token(".", 28),
					new Token("glycogen", 30),
					new Token("amino", 40),
					new Token("-", 45),
					new Token("acids", 46)
				);
		assertTrue(CollectionUtils.isEqualCollection(tokens, expected));
		System.out.println("=== testTokenizeText passed ===");
		
		List<Ngram> ngrams = md.gatherNgrams(tokens, NGRAM_SIZE);
		List<Ngram> expectedNgrams = Arrays.asList(
				new Ngram("our", 0, 3),
				new Ngram("bodies", 4, 6),
				new Ngram("our bodies", 0, 10),
				new Ngram("use", 11, 3),
				new Ngram("our bodies use", 0, 14),
				new Ngram("bodies use", 4, 10),
				new Ngram("carbohydrates", 15, 13),
				new Ngram("bodies use carbohydrates", 4, 24),
				new Ngram("use carbohydrates", 11, 17),
				new Ngram(".", 28, 1),
				new Ngram("use carbohydrates .", 11, 18),
				new Ngram("carbohydrates .", 15, 14),
				new Ngram("glycogen", 30, 8),
				new Ngram("carbohydrates . glycogen", 15, 23),
				new Ngram(". glycogen", 28, 10),
				new Ngram("amino", 40, 5),
				new Ngram(". glycogen amino", 28, 17),
				new Ngram("glycogen amino", 30, 15),
				new Ngram("-", 45, 1),
				new Ngram("glycogen amino -", 30, 16),
				new Ngram("amino -", 40, 6),
				new Ngram("acids", 46, 5),
				new Ngram("amino - acids", 40, 11),
				new Ngram("- acids", 45, 6)
			);
		assertTrue(CollectionUtils.isEqualCollection(ngrams, expectedNgrams));
		System.out.println("=== testGatherNgrams passed ===");
	}
	
	@SuppressWarnings("unused")
	public static void printMemoryRequirements() 
			throws IOException, ParserConfigurationException, SAXException {
		long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		
		TitlesIndex titlesIndex = TitlesIndex.load("/home/marinah/wikipedia/enwiki-titles.txt");
		System.out.println("Loaded titles index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		RedirectPagesIndex redirectIndex = RedirectPagesIndex.load(
				"/home/marinah/wikipedia/enwiki-redirect-normalized.txt");
		System.out.println("Loaded redirect pages index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(
				"/home/marinah/wikipedia/df-index.txt");
		System.out.println("Loaded term df index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(new Configuration(),
				"/home/marinah/wikipedia/tf-idf-entity/");
		entityTFIDFIndex.load(new Path("/home/marinah/wikipedia/tf-idf-entity-index.txt"));
		System.out.println("Loaded entity tf-idf index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		System.out.println("Loaded mention index.");
		usedMemory = printMemoryUsage(usedMemory);
	}
	
	// TO DO:
	public static void testMentionDetection2() 
			throws IOException, ParserConfigurationException, SAXException {
		long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory(); 
		
		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(
				"/home/marinah/wikipedia/df-index.txt");
		System.out.println("Loaded term df index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(new Configuration(),
				"/home/marinah/wikipedia/tf-idf-entity/");
		entityTFIDFIndex.load(new Path("/home/marinah/wikipedia/tf-idf-entity-index"));
		System.out.println("Loaded entity tf-idf index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entities-keyphraseness-index.txt");
		System.out.println("Loaded mention index.");
		usedMemory = printMemoryUsage(usedMemory);
		
		String text = IITBDataset
				.getFileContent("/home/marinah/input/crawledDocs/13Oct08AmitHealth1.txt");
		
		MentionDetection md = new MentionDetection(text, mentionIndex, entityTFIDFIndex, dfIndex);
		
		List<Mention> results = md.solve();
		double minScore = 10;
		for (Mention mention: results) {
			System.out.println(mention);
			if (mention.getKeyphraseness() < minScore) {
				minScore = mention.getKeyphraseness();
			}
		}
		
		System.out.println("min keyphraseness:" + minScore + " total mentions:" + results.size());
/*		List<Ngram> expectedMentions = Arrays.asList(
				new Ngram("our", 0, 3),
				new Ngram("bodies", 4, 6),
				new Ngram("our bodies", 0, 10),
				new Ngram("use", 11, 3),
				new Ngram("our bodies use", 0, 14),
				new Ngram("carbohydrates", 15, 14),
				new Ngram("glycogen", 30, 9),
				new Ngram("amino", 40, 5),
				new Ngram("amino acids", 40, 11)
			);		
		assertTrue(CollectionUtils.isEqualCollection(mentions, expectedMentions));*/
		System.out.println("=== testMentionDetection passed ===");
	}
	
	public static long printMemoryUsage(long alreadyUsed) {
		Runtime runtime = Runtime.getRuntime();
		long usedMemory = runtime.totalMemory() - - runtime.freeMemory();
		long deltaMemory = usedMemory - alreadyUsed;
		System.out.println("Used heap memory:  " + usedMemory);
		System.out.println("Delta heap memory:  " + deltaMemory);
		return usedMemory;
	}
	
	// TO DO: Use assertions.
	public static void testKeyphrasenessMatcher() {
		String keyphraseness = "(11, 12)";
		Pattern p = Pattern.compile("\\((\\d+), (\\d+)\\)");
		Matcher m = p.matcher(keyphraseness);
		m.find();
		int linked = Integer.parseInt(m.group(1));
		int total = Integer.parseInt(m.group(2));
		System.out.println(linked + " " + total);
	} 

	public static void testAnnotationsParsing() 
			throws ParserConfigurationException, SAXException, IOException {
		IITBDataset dataset = 
				new IITBDataset(
						"/home/marinah/wikipedia/enwiki-titles.txt",
						"/home/marinah/wikipedia/enwiki-redirect-normalized.txt"
			  );
		dataset.load(annotationsFilePath, testFilesFolder, false);
		System.out.println("Loaded annotations.");
		
		Set<Annotation> annotations = dataset.getAnnotations("yn_08Oct08_file_23");
		Set<Annotation> expected = new HashSet<Annotation>(
				Arrays.asList(
						new Annotation(646525, 141, 11, "yn_08Oct08_file_23"),
						new Annotation(507385, 153, 11, "yn_08Oct08_file_23"),
						new Annotation(-1, 77, 8, "yn_08Oct08_file_23"),
						new Annotation(646525, 187, 11, "yn_08Oct08_file_23"),
						new Annotation(-1, 69, 7, "yn_08Oct08_file_23"),
						new Annotation(3778, 936, 5, "yn_08Oct08_file_23"),
						new Annotation(152881, 698, 7, "yn_08Oct08_file_23"),
						new Annotation(30067, 710, 11, "yn_08Oct08_file_23"),
						new Annotation(102445, 1104, 13, "yn_08Oct08_file_23"),
						new Annotation(3329659, 977, 4, "yn_08Oct08_file_23"),
						new Annotation(323616, 378, 9, "yn_08Oct08_file_23"),
						new Annotation(20781999, 359, 4, "yn_08Oct08_file_23"),
						new Annotation(-1, 322, 16, "yn_08Oct08_file_23"),
						new Annotation(6675, 309, 12, "yn_08Oct08_file_23"),
						new Annotation(439613, 448, 10, "yn_08Oct08_file_23"),
						new Annotation(26840, 294, 12, "yn_08Oct08_file_23"),
						new Annotation(1831626, 282, 10, "yn_08Oct08_file_23"),
						new Annotation(68761, 554, 9, "yn_08Oct08_file_23"))
				);
		assertEquals(annotations, expected);
		System.out.println("=== testAnnotationsParsing passed ===");
	}
	
	public static void testGatherNgrams() throws IOException {
		AnchorTextIndex anchorTextSet = AnchorTextIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		String tokens[] = new String[] {"united", "states", "elections", "in", "new", "york"};
		Set<String> ngrams = KeyphrasenessIndexBuilder.Map.gatherNgramMentions(
				tokens, 8, anchorTextSet);
		System.out.println(ngrams);
	}
}
