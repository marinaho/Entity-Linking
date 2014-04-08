package evaluation;

import iitb.Annotation;
import iitb.IITBDataset;
import index.MentionIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;
import md.MentionDetection;
import md.Ngram;
import md.Token;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

/*
 * Computes mention detection up to keyphraseness: tokenization, ngram forming, and lookup in the 
 * anchor text dictionary. Keyphraseness is not used to filter out mentions.
 * Checks if any of the ground truth annotations are missing from the set of resulting 
 * mentions.
 */
public class VerifyDictionaryMentions {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations.xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		IITBDataset iitb = new IITBDataset(
				"/home/marinah/wikipedia/enwiki-titles.txt",
				"/home/marinah/wikipedia/enwiki-redirect-normalized.txt");
		iitb.load(annotationsFilePath);
		System.out.println("Loaded IITB dataset.");
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		System.out.println("Loaded mention index.");

		// Maps a filename to a list of found mentions.
		Map<String, List<Mention>> mentionsFound = new HashMap<String, List<Mention>>();
		for (String filename: iitb.getFilenames()) {
			String filePath = FilenameUtils.normalize(testFilesFolder + filename);
			String content = IITBDataset.getFileContent(filePath);
			MentionDetection md = new MentionDetection(content, mentionIndex, null, null);
			
			List<Token> tokens = MentionDetection.tokenizeText(content);
			List<Ngram> ngrams = md.gatherNgrams(tokens, MentionDetection.NGRAM_SIZE);
			mentionsFound.put(filename, md.extractMentions(ngrams, ngrams.size()));
		}
		System.out.println("Gathered mentions.");
		
		int missingMentions = 0, missingEntities = 0, notCanonicalEntity = 0;
		for (Annotation annotation: iitb.getAnnotations()) {
			int result = searchByOffsetLengthFile(annotation, mentionsFound);
			if (result == 1) {
				continue;
			}
			String content = IITBDataset.getFileContent(testFilesFolder + annotation.getFilename());
			String tokenSpan = content
					.substring(annotation.getOffset(), annotation.getOffset() + annotation.getLength());
			String context = content.substring(
					Math.max(annotation.getOffset() - 50, 0), 
					Math.min(annotation.getOffset() + annotation.getLength() + 50, content.length()));
			if (result == -1) {
				System.out.println("Missing: " + annotation + " token span:" + tokenSpan + " context:" + context);
				++missingMentions;
			} else if (result == -2) {
				System.out.println("Missing candidate: " + annotation + " token span:" + tokenSpan + 
						" context:" + context + " id:" );
				++missingEntities;
			} else if (result == -3) {
//				System.out.println("Entity name:" + annotation.getEntityName() + 
//						" not found in wiki titles list; may be disambiguation/list page");
				++notCanonicalEntity;
			}
		}

		System.out.println("Not found mentions:" + missingMentions + " out of:" + 
				iitb.getAnnotations().size() + " total annotations");
		System.out.println("Not found mention with entity:" + missingEntities + " out of:" + 
				iitb.getAnnotations().size() + " total annotations");
		System.out.println("Ground truth annotations resolved to non-canonical entity:" + 
				notCanonicalEntity + " out of:" + iitb.getAnnotations().size() + " total annotations");
	}
	
	/*
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
