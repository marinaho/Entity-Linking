package debug;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;
import md.MentionDetection;
import md.Ngram;
import md.Token;
import normalizer.Normalizer;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import iitb.Annotation;
import iitb.IITBDataset;
import iitb.NameAnnotation;
import index.MentionIndex;
import index.TitleIDsIndex;
import index.TitlesIndex;

public class MentionDetectionDebug {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations (original).xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";

	public static void main(String[] args) 
			throws ParserConfigurationException, SAXException, IOException {
//		displayFalseNegatives(20);
		displayFalsePositives(20);
	}
	
	public static void displayFalseNegatives(int total) 
			throws ParserConfigurationException, SAXException, IOException {
		boolean excludeNonCanonical = true;
		IITBDataset iitb = new IITBDataset(
				"/home/marinah/wikipedia/enwiki-titles.txt",
				"/home/marinah/wikipedia/enwiki-redirect-normalized.txt");
		iitb.load(annotationsFilePath, testFilesFolder, excludeNonCanonical);
		System.out.println("Loaded IITB dataset.");
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		System.out.println("Loaded mention index.");
		
		TitleIDsIndex titleIdsIndex = TitleIDsIndex.load("/home/marinah/wikipedia/enwiki-titles.txt"); 
		System.out.println("Loaded titles index.");
		
		System.out.println("Potential false negatives from ground truth due to low keyphraseness:");
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		TreeSet<Mention> orderedAnnotations = new TreeSet<Mention>();
		for (Annotation annotation: iitb.getAnnotations()) {
			String tokenSpan = Normalizer.normalize(
					annotation.getTokenSpan(
							testFilesFolder + annotation.getFilename()
					)
			);
			if (!mentionIndex.containsKey(tokenSpan)) {
				continue;
			}
			Mention mention = new Mention(tokenSpan, annotation.getOffset(), annotation.getLength());
			mention.setEntityCompatibilityScore(annotation.getEntity(), BigDecimal.ZERO);
			mention.setFilename(annotation.getFilename());
			mention.computeKeyphrasenessAndDF(mentionIndex);
			orderedAnnotations.add(mention);
		}
		
		for (Mention mention: orderedAnnotations) {
			String filename = mention.getFilename();
			int entity = Iterables.get(mention.getCandidateEntities(), 0);
			String entityName = titleIdsIndex.get(entity);
			System.out.println("Mention:" + mention.getNgram() + " Entity:" + entityName +
					" Keyphraseness:" + mention.getKeyphraseness() + " File:" + filename + " Offset:" + 
					mention.getOffset() + " Length:" + mention.getLength() + " Context:" + 
					mention.getContext(testFilesFolder + filename));
			if (--total == 0) {
				System.out.println("More? y / n");
				if (stdin.readLine().equals("y")) {
					total = 20;
				} else {
					break;
				}
			}
		}
	}
	
	public static void displayFalsePositives(int count) 
			throws ParserConfigurationException, SAXException, IOException {
		boolean excludeNonCanonical = true;
		IITBDataset iitb = new IITBDataset(
				"/home/marinah/wikipedia/enwiki-titles.txt",
				"/home/marinah/wikipedia/enwiki-redirect-normalized.txt");
		iitb.load(annotationsFilePath, testFilesFolder, excludeNonCanonical);
		System.out.println("Loaded IITB dataset.");
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		System.out.println("Loaded mention index.");

		Map<String, Set<Mention>> mentionsFound = new HashMap<String, Set<Mention>>();
		for (String filename: iitb.getFilenames()) {
			String filePath = FilenameUtils.normalize(testFilesFolder + filename);
			String content = IITBDataset.getFileContent(filePath);
			MentionDetection md = new MentionDetection(content, mentionIndex, null, null);
			md.setThreshold(1000, false);
			List<Token> tokens = MentionDetection.tokenizeText(content);
			List<Ngram> ngrams = md.gatherNgrams(tokens, MentionDetection.NGRAM_SIZE);
			mentionsFound.put(filename, new HashSet<Mention>(md.extractMentions(ngrams, tokens)));
		}
		
		
		for (Map.Entry<String, Set<Mention>> entry: mentionsFound.entrySet()) {
			for (Mention mention: entry.getValue()) {
				mention.setFilename(entry.getKey());
			}
		}
		
		for (NameAnnotation annotation: iitb.getNameAnnotations()) {
			removeAnnotation(annotation, mentionsFound);
		}
		
		TreeSet<Mention> falsePositives = new TreeSet<Mention>();
		for (Set<Mention> set: mentionsFound.values()) {
			falsePositives.addAll(set);
		}
		
		HashSet<String> visited = new HashSet<String>(); 
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		for (Mention mention: falsePositives.descendingSet()) {
			String visit = Joiner.on("\t").join(mention.getNgram(), mention.getFilename());
			if (visited.contains(visit)) {
				continue;
			}
			visited.add(visit);
			System.out.println("False positive:" + mention.getOriginalNgram() +  " File:" + 
					mention.getFilename() + " Keyphraseness:" + mention.getKeyphraseness() + " Context:" + 
					mention.getContext(testFilesFolder + mention.getFilename()) + " Offset:" + 
					mention.getOffset() + " Length:" + mention.getLength());
			--count;
			if (count == 0) {
				System.out.println("More?  y / n.");
				if (stdin.readLine().equals("y")) {
					count = 10;
				} else {
					break;
				}
			}
		}
		stdin.close();
	}
	
	public static void removeAnnotation(NameAnnotation input, Map<String, Set<Mention>> map) {
		if (input.getEntity() == TitlesIndex.NOT_CANONICAL_TITLE) {
			return;
		}
		String filename = input.getFilename();
		for (Mention mention: new HashSet<Mention>(map.get(filename))) {
			if (mention.getOriginalNgram().equals(input.getName()) && 
					mention.getFilename().equals(filename) &&
					Arrays.asList(mention.getCandidateEntities()).contains(input.getEntity())) {
				map.get(filename).remove(mention);
			}
		}
	}
}
