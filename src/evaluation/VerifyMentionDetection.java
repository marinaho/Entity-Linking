package evaluation;

import iitb.Annotation;
import iitb.IITBDataset;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;
import md.MentionDetection;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;

public class VerifyMentionDetection {
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
		
		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(
				new Configuration(), "/home/marinah/wikipedia/tf-idf-entity");
		entityTFIDFIndex.load(new Path("/home/marinah/wikipedia/tf-idf-entity-index.txt"));
		System.out.println("Loaded tf-idf entity index.");
		
		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(
				"/home/marinah/wikipedia/df-index.txt");
		System.out.println("Loaded term document frequency index.");

		// Maps a filename to a list of found mentions.
		Map<String, List<Mention>> mentionsFound = new HashMap<String, List<Mention>>();
		for (String filename: iitb.getFilenames()) {
			String filePath = FilenameUtils.normalize(testFilesFolder + filename);
			String content = IITBDataset.getFileContent(filePath);
			MentionDetection md = new MentionDetection(content, mentionIndex, entityTFIDFIndex, dfIndex);
			mentionsFound.put(filename, md.solve());
			System.out.println("Found mentions for text: " + filename);
		}
		System.out.println("Gathered mentions.");
		
		int missingMentions = 0, missingEntities = 0, notCanonicalEntity = 0;
		for (Annotation annotation: iitb.getAnnotations()) {
			int result = searchByOffsetLengthFile(annotation, mentionsFound);
			if (result == 1) {
				continue;
			}
			if (result == -1) {
				++missingMentions;
			} else if (result == -2) {
				++missingEntities;
			} else if (result == -3) {
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
