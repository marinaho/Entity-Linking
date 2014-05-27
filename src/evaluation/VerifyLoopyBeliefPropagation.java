package evaluation;

import iitb.Annotation;
import iitb.IITBDataset;
import iitb.NameAnnotation;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;
import index.TitleIDsIndex;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import loopybeliefpropagation.LoopyBeliefPropagation;
import loopybeliefpropagation.ScorerBasic;
import md.Mention;
import md.MentionDetection;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

public class VerifyLoopyBeliefPropagation {
	private static int iterations = 75;
	
	private static final String annotationsFilePath = "/home/marinah/input/CSAW_Annotations (original).xml";
	private static final String testFilesFolder = "/home/marinah/input/crawledDocs/";
	private static final String titlesFilePath = "/home/marinah/wikipedia/enwiki-titles.txt";
	private static final String termDFIndexPath = "/home/marinah/wikipedia/df-index.txt";
	private static final String entityTfIDFFilesPath = "/home/marinah/wikipedia/tf-idf-entity";
	private static final String entityTfIDFIndexPath = 
			"/home/marinah/wikipedia/tf-idf-entity-index.txt";
	private static final String redirectsFilePath = 
			"/home/marinah/wikipedia/enwiki-redirect-normalized.txt";
	private static final String mentionIndexPath = 
			"/home/marinah/wikipedia/mention-entity-keyphraseness-limited25.txt";
	private static final String entityLinksIndexPath = 
			"/home/marinah/wikipedia/entity-entity-index.txt";
	
	public static TitleIDsIndex titleIdsIndex;
	
	public static PrintWriter out;
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		System.out.println("VerifyLoopyBeliefPropagation - mention index: " + mentionIndexPath);
		
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.INFO);
		@SuppressWarnings("rawtypes")
		Enumeration allLoggers = Logger.getRootLogger().getLoggerRepository().getCurrentCategories();
		while (allLoggers.hasMoreElements()){
			Category tmpLogger = (Category) allLoggers.nextElement();
		  tmpLogger.setLevel(Level.INFO);
		}
		
		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(entityTfIDFFilesPath);
		entityTFIDFIndex.load(new Path(entityTfIDFIndexPath));
		System.out.println("Loaded tf-idf entity index.");
		    
		IITBDataset iitb = new IITBDataset(titlesFilePath, redirectsFilePath);
		iitb.load(annotationsFilePath, testFilesFolder, true);
		System.out.println("Loaded IITB dataset.");
		
		MentionIndex mentionIndex = MentionIndex.load(mentionIndexPath);
		System.out.println("Loaded mention index.");
		
		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(termDFIndexPath);
		System.out.println("Loaded term document frequency index.");

		EntityLinksIndex entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		System.out.println("Loaded entity links index.");
		
		titleIdsIndex = TitleIDsIndex.load(titlesFilePath); 
		System.out.println("Loaded title ids index.");
 
		for (double threshold = 0.9; threshold >= 0.1; threshold -= 0.01) {
			System.out.println("Threshold:" + threshold);
			double averagePrecision = 0.0;
			double averageRecall = 0.0;
			Set<NameAnnotation> solution = new HashSet<NameAnnotation>();
			int total = 0;
			for (String filename: iitb.getFilenames()) {
				System.out.println("Solving for document:" + filename + " Number:" + (++total));			
				String filePath = FilenameUtils.normalize(testFilesFolder + filename);
				String content = IITBDataset.getFileContent(filePath);
				MentionDetection md = new MentionDetection(content, mentionIndex, entityTFIDFIndex, 
						dfIndex);
				md.setThreshold(threshold, true);
				List<Mention> mentions = md.solve();
				ScorerBasic scorer = new ScorerBasic(mentions, entityLinksIndex);
				LoopyBeliefPropagation lbp = new LoopyBeliefPropagation(mentions, iterations, scorer);
				lbp.solve();
				Set<NameAnnotation> currentSolution = lbp.getSolutionNameAnnotations(filename);
				solution.addAll(currentSolution);
				
				Verifier<NameAnnotation> verifier = new Verifier<NameAnnotation>();
				verifier.computeResults(currentSolution, iitb.getNameAnnotations(filename));
				
				System.out.println("Good\n" + NameAnnotation.outputAnnotations(
						verifier.getCorrectAnnotations(), titleIdsIndex));
				System.out.println("False positives:\n" + NameAnnotation.outputAnnotations(
						verifier.getWrongAnnotations(), titleIdsIndex));
				System.out.println("False negatives:\n" + NameAnnotation.outputAnnotations(
						verifier.getNotFoundAnnotations(), titleIdsIndex));
				System.out.println("Precision: " + verifier.getPrecision());
				System.out.println("Recall: " + verifier.getRecall());		
				
				averagePrecision += verifier.getPrecision();
				averageRecall += verifier.getRecall();
			}
			averagePrecision /= iitb.getNumDocs();
			averageRecall /= iitb.getNumDocs();
			
			Verifier<NameAnnotation> verifier = new Verifier<NameAnnotation>();
			verifier.computeResults(solution, iitb.getNameAnnotations());
		
			System.out.println("=============== RESULTS =================");
			System.out.println("Precision: " + verifier.getPrecision());
			System.out.println("Recall: " + verifier.getRecall());		
			System.out.println("Average precision: " + averagePrecision);
			System.out.println("Average recall: " + averageRecall);
		}
	}
	
	public static String outputAnnotations(Set<Annotation> set, String content) {
		String result = "";
		for (Annotation annotation: set) {
			int offset = annotation.getOffset();
			int length = annotation.getLength();
			String tokenSpan = content.substring(offset, offset + length);
			String context =  content.substring(
					Math.max(0, offset - 50), 
					Math.min(content.length(), offset + length)
			);
			result += "Token:" + tokenSpan + " Entity:" + titleIdsIndex.get(annotation.getEntity()) +
					"\nContext:" + context;
		}
		return result;
	}
}
