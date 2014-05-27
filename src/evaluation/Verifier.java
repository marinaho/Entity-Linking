package evaluation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * From a set on annotated token spans and ground truth annotations computes precision, recall and
 * lists of correct, wrong and unfound annotations. 
 */
public class Verifier<T> {
	private double precision;
	private double recall;
	List<T> correctAnnotations;
	List<T> wrongAnnotations;
	List<T> notFoundAnnotations;
	
	public Verifier() {
		correctAnnotations = new ArrayList<T>();
		wrongAnnotations =  new ArrayList<T>();
		notFoundAnnotations = new ArrayList<T>();
	}
	
	public void computeResults(Set<T> solution, Set<T> groundTruth) {
		int toFind = groundTruth.size();
		int found = solution.size();
		int good = 0;
		
		for (T annotation: solution) {
			if (groundTruth.contains(annotation)) {
				++good;
				correctAnnotations.add(annotation);
			} else {
				wrongAnnotations.add(annotation);
			}
			
		}
		
		for (T annotation: groundTruth) {
			if (!solution.contains(annotation)) {
				notFoundAnnotations.add(annotation);
			}
		}
		
		precision = found > 0 ? (double) good / found : 1;
		recall = toFind > 0 ? (double) good / toFind : 1;
	}
	
	public double getPrecision() {
		return precision;
	}
	
	public double getRecall() {
		return recall;
	}
	
	public List<T> getCorrectAnnotations() {
		return correctAnnotations;
	}
	
	public List<T> getWrongAnnotations() {
		return wrongAnnotations;
	}
	
	public List<T> getNotFoundAnnotations() {
		return notFoundAnnotations;
	}
	
	@Override
	public String toString() {
		return "Precision: " + precision + "Recall:" + recall;
	}
}
