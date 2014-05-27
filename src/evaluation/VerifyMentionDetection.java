package evaluation;

import iitb.Annotation;
import iitb.IITBDataset;
import index.MentionEntitiesFrequencyIndex;
import index.MentionIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;
import md.MentionDetection;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

/**
 * Computes maximum possible recall after running mention detection and keeping the top x% scoring
 * mentions by keyphraseness.
 */
public class VerifyMentionDetection {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations (original).xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	private static final String titlesFilePath = "/mnt/local/marinah/wikipedia/enwiki-titles.txt";
	private static final String redirectsFilePath = 
			"/mnt/local/marinah/wikipedia/enwiki-redirect-normalized.txt";
	private static final String mentionIndexPath = 
			"/mnt/local/marinah/wikipedia/mek-top.txt";
	private static final String mentionFreqIndexPath = 
			"/mnt/local/marinah/wikipedia/mek-top-freq.txt";
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		System.out.println("# VerifyMentionDetection");
		boolean excludeNonCanonical = true; 
		
		IITBDataset iitb = new IITBDataset(titlesFilePath, redirectsFilePath);
		iitb.load(annotationsFilePath, testFilesFolder, excludeNonCanonical);
		System.out.println("# Loaded IITB dataset.");
		
		System.out.println("# Loading mention index:" + mentionFreqIndexPath);
		MentionEntitiesFrequencyIndex mentionIndex = 
				MentionEntitiesFrequencyIndex.load(mentionFreqIndexPath);

		System.out.println("# Threshold | Max Avg Precision | Max Avg Recall | Max F1 score |"
				+ " Missing annotations | Good | Total found | Total ground truth");
		
		ArrayList<Double> thresholds = new ArrayList<Double>();
		thresholds.add(0.00005);
		thresholds.add(0.0001);
		thresholds.add(0.0005);
		for (double t = 0.001; t < 0.01; t += 0.001) {
			thresholds.add(t);
		}
		for (double t = 0.01; t <= 1; t += 0.01) {
			thresholds.add(t);
		}
		for (double threshold: thresholds) {
			double averagePrecision = 0;
			double averageRecall = 0;
			int missingAnnotations = 0;
			int good = 0;
			int found = 0;
			
			for (String filename: iitb.getFilenames()) {
				String filePath = FilenameUtils.normalize(testFilesFolder + filename);
				String content = IITBDataset.getFileContent(filePath);
				
				MentionDetection md = new MentionDetection(content, mentionIndex, null, null);
				md.setThreshold(threshold, true);
				List<Mention> candidateMentions = md.getCandidateMentions(threshold);
				
				VerifierMentionDetection verifier = new VerifierMentionDetection();
				verifier.computeResults(candidateMentions, iitb.getNameAnnotations(filename));
				averagePrecision += verifier.getPrecision();
				averageRecall += verifier.getRecall();
				missingAnnotations += verifier.getNotFoundAnnotations().size();
				good += verifier.getCorrectAnnotations().size();
				found += candidateMentions.size();
			}
			averagePrecision /= iitb.getNumDocs();
			averageRecall /= iitb.getNumDocs();
			double maxF1score = (averagePrecision + averageRecall) / 2;
			
			System.out.format("%10.5f %10.2f %10.2f %10.2f %d %d %d %d\n", threshold, 
					averagePrecision * 100, averageRecall * 100, maxF1score * 100, missingAnnotations, good, 
					found, iitb.getNameAnnotations().size());
			}
	}
	
	/**
	 * Returns:
	 *  1 if mention is present in list
	 * -1 if a mention with the annotation offset, length and file does not exist
	 * -2 if a mention exists but does not have the annotations entity in the candidates list
	 * -3 if annotation entity is not in the wiki titles index (may be a disambiguation/list page)
	 */
	public static int searchByOffsetLengthFile(Annotation input, Map<String, List<Mention>> map) {
		if (input.getEntity() == -1) {
			return -3;
		}
		for (Mention mention: map.get(input.getFilename())) {
			if (mention.getOffset() == input.getOffset() && 
					mention.getLength() == input.getLength()) {
				if (Arrays.asList(mention.getCandidateEntities()).contains(input.getEntity())) {
					return 1;
				}
				return -2;
			}
		}
		return -1;
	}
}
