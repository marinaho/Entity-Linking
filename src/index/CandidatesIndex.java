package index;

import java.util.Map;

import md.Mention;
import edu.umd.cloud9.io.pair.PairOfInts;

public interface CandidatesIndex<T> extends Map<String, T> {
	public PairOfInts getKeyphraseness(String key);
	
	public Integer[] getCandidateEntities(String key);
	
	public Integer[] getCandidateEntities(Mention mention);
	
	public Integer getCandidateEntitiesCount(String name);
}