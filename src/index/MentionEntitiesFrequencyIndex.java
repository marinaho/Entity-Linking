package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import data.CandidatesEntry;
import data.NameEntry;
import knowledgebase.MentionEntitiesKeyphrasenessIndexBuilder;
import md.Mention;
import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * Maps mentions to keyphraseness and candidate entities list.
 * The file loaded should be produced by @see knowledgebase.MentionEntitiesKeyphrasenessIndexBuilder
 */
public class MentionEntitiesFrequencyIndex extends HashMap<String, NameEntry> 
		implements CandidatesIndex<NameEntry> {
	private static final long serialVersionUID = 3037740362623413024L;
	private static final int INITIAL_SIZE = 9202002;
	
	public static final int MINIMUM_COUNT = 5;
	public static final String SEPARATOR = "\t";
	
	public MentionEntitiesFrequencyIndex(int size) {
    super(size);
	} 
	
	public static MentionEntitiesFrequencyIndex load(String path)  throws IOException {
		MentionEntitiesFrequencyIndex dictionary = new MentionEntitiesFrequencyIndex(
				INITIAL_SIZE * 4 / 3 + 1);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split(MentionEntitiesKeyphrasenessIndexBuilder.SEPARATOR, 4);
      String name = elements[0];
      int linkedDocs = Integer.parseInt(elements[1]);
      int totalDocs = Integer.parseInt(elements[2]);
      // Skip mentions contained in less than MINIMUM_COUNT docs.
      if (totalDocs < MINIMUM_COUNT || linkedDocs == 0) {
      	continue;
      }
      CandidatesEntry candidates = new CandidatesEntry(elements[3]);

      dictionary.put(name, new NameEntry(linkedDocs, totalDocs, candidates));
    }
    in.close();
    
    return dictionary;
  }
	
	public PairOfInts getKeyphraseness(String name) {
		NameEntry entry = get(name);
		return new PairOfInts(entry.getLinkedDocs(), entry.getTotalDocs());
	}
	
	public Integer[] getCandidateEntities(String name) {
		return get(name).getCandidateEntities().toArray(new Integer[0]);
	}
	
	public Integer[] getCandidateEntities(Mention mention) {
		return getCandidateEntities(mention.getNgram());
	}
	
	public double getCandidateProbability(String name, Integer candidate) {
		return get(name).getCandidateProbability(candidate);
	}
	
	public int getCandidateFrequency(String name, Integer candidate) {
		return get(name).getCandidateFrequency(candidate);
	}
	
	public Integer getMostFrequentEntity(String name) {
		return get(name).getMostFrequentEntity();
	}
	
	public Integer getCandidateEntitiesCount(String name) {
		return get(name).getCandidateEntitiesCount();
	}
}
