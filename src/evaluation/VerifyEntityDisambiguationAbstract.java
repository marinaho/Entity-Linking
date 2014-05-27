package evaluation;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;
import md.MentionDetection;
import md.Token;
import normalizer.Normalizer;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import iitb.Annotation;
import iitb.IITBDataset;
import iitb.NameAnnotation;
import index.CandidatesIndex;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.TermDocumentFrequencyIndex;
import index.TitleIDsIndex;

public abstract class VerifyEntityDisambiguationAbstract {
	public static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations (original).xml";
	public static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	public static final String titlesFilePath = "/mnt/local/marinah/wikipedia/enwiki-titles.txt";
	public static final String mentionIndexPath = "/mnt/local/marinah/wikipedia/mek-top.txt";
	public static final String mentionFreqIndexPath = 
			"/mnt/local/marinah/wikipedia/mek-top-freq.txt";
	public static final String termDFIndexPath = "/mnt/local/marinah/wikipedia/df-index.txt";
	public static final String entityTfIDFFilesPath = "/mnt/local/marinah/wikipedia/tf-idf-entity";
	public static final String entityTfIDFIndexPath = 
			"/mnt/local/marinah/wikipedia/tf-idf-entity-index.txt";
	public static final String redirectsFilePath = 
			"/mnt/local/marinah/wikipedia/enwiki-redirect-normalized.txt";
	public static final String entityLinksIndexPath = 
			"/mnt/local/marinah/wikipedia/entity-entity-index.txt";
	
	public IITBDataset iitb;
	public EntityTFIDFIndex entityTFIDFIndex;
	public TermDocumentFrequencyIndex dfIndex;
	public EntityLinksIndex entityLinksIndex;
	public TitleIDsIndex titleIdsIndex;
	
	public HashSet<String> missing;
	
	private boolean skipZeroCompatibilityMentions = false;
	
	public VerifyEntityDisambiguationAbstract() {
		missing = new HashSet<String>();
	}
	
	public void configureLogging() {
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		@SuppressWarnings("rawtypes")
		Enumeration allLoggers = Logger.getRootLogger().getLoggerRepository().getCurrentCategories();
		while (allLoggers.hasMoreElements()){
			Category tmpLogger = (Category) allLoggers.nextElement();
		  tmpLogger.setLevel(Level.INFO);
		}
	}
	
	public void loadIndices() throws ParserConfigurationException, SAXException, IOException {
		iitb = new IITBDataset(titlesFilePath, redirectsFilePath);
		iitb.load(annotationsFilePath, testFilesFolder, true);
		System.out.println("Loaded IITB dataset.");
		
		entityTFIDFIndex = new EntityTFIDFIndex(entityTfIDFFilesPath);
		entityTFIDFIndex.load(new Path(entityTfIDFIndexPath));
		System.out.println("Loaded tf-idf entity index:" + entityTfIDFFilesPath);
		
		dfIndex = TermDocumentFrequencyIndex.load(termDFIndexPath);
		System.out.println("Loaded term document frequency index:" + termDFIndexPath);

		entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		System.out.println("Loaded entity links index:" + entityLinksIndexPath);
		
		titleIdsIndex = TitleIDsIndex.load(titlesFilePath); 
		System.out.println("Loaded title ids index:" + titlesFilePath);
	} 
	
	public List<Mention> getGroundTruthNameAnnotations(String filename, CandidatesIndex mentionIndex) 
			throws IOException {
		String filePath = FilenameUtils.normalize(testFilesFolder + filename);
		String content = IITBDataset.getFileContent(filePath);
		
		MentionDetection md = new MentionDetection(content, mentionIndex, entityTFIDFIndex, dfIndex);
		List<Token> tokens = MentionDetection.tokenizeText(content);
		List<Mention> groundTruthMentions = new ArrayList<Mention>();
		for (Annotation annotation: iitb.getAnnotations(filename)) {
			int offset = annotation.getOffset();
			int length = annotation.getLength();
			String tokenSpan = content.substring(offset, offset + length);
			String normalizedTokenSpan = Normalizer.normalize(tokenSpan);
			if (!mentionIndex.containsKey(normalizedTokenSpan)) {
				missing.add(Joiner.on("\t").join(filename, normalizedTokenSpan));
				continue;
			}
			Integer[] candidates;
			try {
				candidates = mentionIndex.getCandidateEntities(normalizedTokenSpan);
			} catch (Exception e) {
				throw new IllegalArgumentException("Name:" + normalizedTokenSpan);
			}
			if (!Arrays.asList(candidates).contains(annotation.getEntity())) {
					missing.add(Joiner.on("\t").join(filename, normalizedTokenSpan));
					continue;
			}
			Mention mention = new Mention(normalizedTokenSpan, offset, length);
			mention.setOriginalNgram(tokenSpan);
			mention.computeKeyphrasenessAndDF(mentionIndex);
			for (Integer entity: candidates) {
				BigDecimal score = md.getLocalMentionEntityCompatibility(mention, entity, tokens);
				mention.setEntityCompatibilityScore(entity, score);
			}
			if (skipZeroCompatibilityMentions && 
					mention.computeSumCompatibilities().compareTo(BigDecimal.ZERO) == 0) {
				continue;
			}
			groundTruthMentions.add(mention);
		}
		md.computeImportance(groundTruthMentions);
		return groundTruthMentions;
	}
	
	public Verifier<NameAnnotation> verifyNameAnnotations(Set<NameAnnotation> results) 
			throws IOException {
		String filename = Iterables.get(results, 0).getFilename();
		Verifier<NameAnnotation> verifier = new Verifier<NameAnnotation>();
		verifier.computeResults(results, iitb.getNameAnnotations(filename));
		
		System.out.println(filename + " Precision:" + verifier.getPrecision() + " Recall:" + 
				verifier.getRecall());
		System.out.println("Good\n" + NameAnnotation.outputAnnotations(
				verifier.getCorrectAnnotations(), titleIdsIndex));
		System.out.println("False positives:\n" + NameAnnotation.outputAnnotations(
				verifier.getWrongAnnotations(), titleIdsIndex));
		System.out.println("False negatives:\n" + NameAnnotation.outputAnnotations(
				verifier.getNotFoundAnnotations(), titleIdsIndex));

		return verifier;
	}
	
	public double getMaximumAchievableRecall() {
		return 1 - (double) missing.size() / iitb.getNameAnnotationsCount();
	}
	
	public void setSkipZeroCompatibilityMentions() {
		skipZeroCompatibilityMentions = true;
	}
}
