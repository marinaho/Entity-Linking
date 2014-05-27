package baseline;

import index.MentionEntitiesFrequencyIndex;

import java.util.HashMap;
import java.util.List;

import md.Mention;

/**
 * Disambiguates a mention to the most frequent candidate entity.
 */ 
public class MostFrequentEntity {
	private MentionEntitiesFrequencyIndex mentionIndex;
	private HashMap<Mention, Integer> solution;
	
	public MostFrequentEntity(MentionEntitiesFrequencyIndex mentionIndex) {
		this.mentionIndex = mentionIndex;
	}
	
	public HashMap<Mention, Integer> solve(List<Mention> mentions) {
		solution = new HashMap<Mention, Integer>();
		for (Mention mention: mentions) {
			solution.put(mention, mentionIndex.getMostFrequentEntity(mention.getNgram()));
		}
		return solution;
	}
}
