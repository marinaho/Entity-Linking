package evaluation;
import iitb.Annotation;
import iitb.IITBDataset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import md.MentionDetection;
import md.Ngram;
import md.Token;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

/**
 * Check which IITB annotations do not occur in the tokenized ngrams of the inputs.
 * This should identify differences in tokenizations.
 * The candidate entities are not computed and compared at this point.
 */
public class VerifyNgrams {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations.xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	private static String titlesIndexPath = "/home/marinah/wikipedia/enwiki-titles.txt";
	private static String redirectsIndexPath = 
			"/home/marinah/wikipedia/enwiki-redirect-normalized.txt";
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		IITBDataset iitb = new IITBDataset(titlesIndexPath, redirectsIndexPath);
		iitb.load(annotationsFilePath, testFilesFolder, false);
		
		List<Annotation> ngramsFound = new ArrayList<Annotation>();
		for (String filename: iitb.getFilenames()) {
			String filePath = FilenameUtils.normalize(testFilesFolder + filename);
			String content = IITBDataset.getFileContent(filePath);
			MentionDetection md = new MentionDetection(content, null, null, null);
			
			List<Token> tokens = MentionDetection.tokenizeText(content);
			for (Ngram ngram: md.gatherNgrams(tokens, MentionDetection.NGRAM_SIZE)) {
				ngramsFound.add(
						new Annotation(
								-2, // Entity not determined yet.
								ngram.getOffset(),
								ngram.getLength(),
								filename
						)
				);
			}
		}
		
		int missingCount = 0;
		for (Annotation annotation: iitb.getAnnotations()) {
			if (!searchByOffsetLengthFile(annotation, ngramsFound)) {
				System.out.println("Missing: " + annotation);
				++missingCount;
			}
		}

		System.out.println("Not found total:" + missingCount + " out of:" + 
				iitb.getAnnotations().size() + " total annotations");
	}
	
	public static boolean searchByOffsetLengthFile(Annotation input, List<Annotation> list) {
		for (Annotation annotation: list) {
			if (annotation.getOffset() == input.getOffset() && 
					annotation.getLength() == input.getLength() &&
					annotation.getFilename().equals(input.getFilename())) {
				return true;
			}
		}
		return false;
	}
}
