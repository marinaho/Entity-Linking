package evaluation;

import iitb.NameAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import md.Mention;

/**
 * Verifies a list of candidate mentions against a collection of name annotations.
 * The annotations and mentions must come from the same file.
 */
public class VerifierMentionDetection {
	private double precision;
	private double recall;
	List<NameAnnotation> goodAnnotations;
	Set<String> wrongMentions;
	List<NameAnnotation> notFoundAnnotations;
	
	public VerifierMentionDetection() {
		goodAnnotations = new ArrayList<NameAnnotation>();
		wrongMentions =  new HashSet<String>();
		notFoundAnnotations = new ArrayList<NameAnnotation>();
	}
	
	public void computeResults(List<Mention> mentions, Collection<NameAnnotation> groundTruth) 
			throws IOException {
		int good = 0;
		
		for (Mention mention: mentions) {
			if (!wrongMentions.contains(mention.getOriginalNgram()) && 
					!searchMention(mention, groundTruth)) {
				wrongMentions.add(mention.getOriginalNgram());
			}
		}
		
		for (NameAnnotation annotation: groundTruth) {
			if (searchNameAnnotation(annotation, mentions)) {
				++good;
				goodAnnotations.add(annotation);
			} else {
				notFoundAnnotations.add(annotation);
			}
		}
		
		int found = good + wrongMentions.size();
		precision = found > 0 ? (double) good / found : 1.0;
		recall = groundTruth.size() > 0 ? (double) good / groundTruth.size() : 1.0;
	}
	
	public boolean searchMention(Mention mention, Collection<NameAnnotation> collection) {
		for (NameAnnotation nameAnnotation: collection) {
			if (nameAnnotation.getName().equals(mention.getOriginalNgram()) && 
					mention.getCandidateEntities().contains(nameAnnotation.getEntity())) {
				return true;
			}
		}
		return false;
	}
	
	public boolean searchNameAnnotation(NameAnnotation annotation, Collection<Mention> mentions) 
			throws IOException {
		for (Mention mention: mentions) {
			if (mention.getOriginalNgram().equals(annotation.getName()) &&
					mention.getCandidateEntities().contains(annotation.getEntity())) {
				return true;
			}
		}
		return false;
	}
	
	public double getPrecision() {
		return precision;
	}
	
	public double getRecall() {
		return recall;
	}
	
	public List<NameAnnotation> getCorrectAnnotations() {
		return goodAnnotations;
	}
	
	public Set<String> getWrongAnnotations() {
		return wrongMentions;
	}
	
	public List<NameAnnotation> getNotFoundAnnotations() {
		return notFoundAnnotations;
	}
	
	@Override
	public String toString() {
		return "Precision: " + precision + "Recall:" + recall;
	}
}
