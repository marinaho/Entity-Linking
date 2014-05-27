package data;

import java.util.Set;

public class NameEntry {
	private CandidatesEntry candidates;
	private int linkedDocs;
	private int totalDocs;
	
	public NameEntry(int linkedDocs, int totalDocs, CandidatesEntry candidates) {
		this.linkedDocs = linkedDocs;
		this.totalDocs = totalDocs;
		this.candidates = candidates;
	}
	
	public int getLinkedDocs() {
		return linkedDocs;
	}
	
	public int getTotalDocs() {
		return totalDocs;
	}
	
	public Set<Integer> getCandidateEntities() {
		return candidates.getCandidates();
	}
	
	public Integer getCandidateEntitiesCount() {
		return candidates.getCandidatesCount();
	}
	
	public double getCandidateProbability(Integer candidate) {
		int candidateFrequency = candidates.getCandidateFrequency(candidate);
		int totalFrequency = candidates.getTotalFrequency();
		return (double) candidateFrequency / totalFrequency;
	}
	
	public int getCandidateFrequency(Integer candidate) {
		return candidates.getCandidateFrequency(candidate);
	}
	
	public Integer getMostFrequentEntity() {
		return candidates.getMostFrequentEntity();
	}
}
