package evaluation;

import iitb.Annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Verifier {
	private double precision;
	private double recall;
	List<Annotation> correctAnnotations;
	List<Annotation> wrongAnnotations;
	List<Annotation> notFoundAnnotations;
	
	public Verifier() {
		correctAnnotations = new ArrayList<Annotation>();
		wrongAnnotations =  new ArrayList<Annotation>();
		notFoundAnnotations = new ArrayList<Annotation>();
	}
	
	public void computeResults(Set<Annotation> solution, Set<Annotation> groundTruth) {
		int toFind = groundTruth.size();
		int found = solution.size();
		int good = 0;
		
		for (Annotation annotation: solution) {
			if (groundTruth.contains(annotation)) {
				++good;
				correctAnnotations.add(annotation);
			} else {
				wrongAnnotations.add(annotation);
			}
			
		}
		
		for (Annotation annotation: groundTruth) {
			if (!solution.contains(annotation)) {
				notFoundAnnotations.add(annotation);
			}
		}
		
		precision = (double) good / found;
		recall = (double) good / toFind;
	}
	
	public double getPrecision() {
		return precision;
	}
	
	public double getRecall() {
		return recall;
	}
	
	public List<Annotation> getCorrectAnnotations() {
		return correctAnnotations;
	}
	
	public List<Annotation> getWrongAnnotations() {
		return wrongAnnotations;
	}
	
	public List<Annotation> getNotFoundAnnotations() {
		return notFoundAnnotations;
	}
	
	@Override
	public String toString() {
		return "Precision: " + precision + "Recall:" + recall;
	}
}
