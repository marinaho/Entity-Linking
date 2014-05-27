package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

public class CandidatesEntry implements Writable {
	public static final String CANDIDATE_SEPARATOR = "\t";
	public static final String ENTRY_SEPARATOR = ",";
	
	// Total number of times the corresponding name is linked (may be bigger than the number of 
	// documents in which it is linked).
	private int nameFrequency;
	private HashMap<Integer, Integer> topCandidates;
	
	public CandidatesEntry(int nameFrequency) {
		this.nameFrequency = nameFrequency;
		this.topCandidates = new HashMap<Integer, Integer>();
	}
	
	public CandidatesEntry(int totalFrequency, HashMap<Integer, Integer> topCandidates) {
		this.nameFrequency = totalFrequency;
		this.topCandidates = topCandidates;
	}
	
	/**
	 * Recreates an object from the textual representation obtained by using toString() method.
	 */
	public CandidatesEntry(String text) {
		String[] parts = StringUtils.split(text, CANDIDATE_SEPARATOR);
		nameFrequency = Integer.parseInt(parts[0]);
		this.topCandidates = new HashMap<Integer, Integer>();
		for (String part: Arrays.asList(parts).subList(1, parts.length)) {
			String subparts[] = StringUtils.split(part, ENTRY_SEPARATOR);
			int entity = Integer.parseInt(subparts[0]);
			int frequency = Integer.parseInt(subparts[1]);
			topCandidates.put(entity, frequency);
		}
	}
	
	public int getTotalFrequency() {
		return nameFrequency;
	}
	
	public Set<Integer> getCandidates() {
		return topCandidates.keySet();
	}
	
	public int getCandidatesCount() {
		return topCandidates.size();
	}
	
	public Integer getMostFrequentEntity() {
		int bestEntity = -1;
		int bestFrequency = -1;
		for (Map.Entry<Integer, Integer> entry: topCandidates.entrySet()) {
			int frequency = entry.getValue();
			if (frequency > bestFrequency) {
				bestFrequency = frequency;
				bestEntity = entry.getKey();
			}
		}
		return bestEntity;
	}
	
	public Integer getCandidateFrequency(Integer entity) {
		return topCandidates.get(entity);
	}
	
	public void setCandidateFrequency(Integer entity, Integer frequency) {
		topCandidates.put(entity, frequency);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nameFrequency = in.readInt();
		int numCandidates = in.readInt();
		while (numCandidates-- != 0) {
			topCandidates.put(in.readInt(), in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(nameFrequency);
		out.writeInt(topCandidates.size());
		for (Map.Entry<Integer, Integer> entry: topCandidates.entrySet()) {
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());
		}
	}
	
	@Override
	public String toString() {
		String candidates = 
				Joiner.on(CANDIDATE_SEPARATOR).withKeyValueSeparator(ENTRY_SEPARATOR).join(topCandidates);
		return Joiner.on(CANDIDATE_SEPARATOR).join(nameFrequency, candidates);
	}
}
