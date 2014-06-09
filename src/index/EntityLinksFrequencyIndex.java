package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import data.EntityLinksEntry;
import edu.umd.cloud9.io.pair.PairOfInts;

/**
 * Stores a mapping of (entity1, number of entities that link to the key).
 * Can be used to compute semantic relatedness between two entities.
 * The file loaded should be produced by @see knowledgebase.EntityMentionIndexBuilder .
 */
public class EntityLinksFrequencyIndex extends HashMap<Integer, EntityLinksEntry> 
		implements LinksIndex {
	private static final long serialVersionUID = 2718316266900207592L;
	private static final int INITIAL_SIZE = 3794882;
	public static final String SEPARATOR = "\t";
	
	public EntityLinksFrequencyIndex(int size) {
    super(size);
	}
	
	public static EntityLinksFrequencyIndex load(String path)  throws IOException {
		EntityLinksFrequencyIndex dictionary = new EntityLinksFrequencyIndex(INITIAL_SIZE * 4 / 3 + 1);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
    	String[] tokens = StringUtils.split(line, SEPARATOR, 2);
    	int entity = Integer.parseInt(tokens[0]);
    	EntityLinksEntry entry = new EntityLinksEntry(tokens[1]);
    	dictionary.put(entity, entry);
    }
    in.close();
    
    return dictionary;
  }
	
	public int getPopularity(int entity) {
		return 1 + (containsKey(entity) ? get(entity).getTotalFrequency() : 0);
	}
	
	public int getCocitation(int entity1, int entity2) {
		if (!containsKey(entity1) || !containsKey(entity2)) {
			return 0;
		}
		
		if (entity1 == entity2) {
			int result = 0;
			for (PairOfInts link: get(entity1).getLinks()) {
				int frequency = link.getRightElement();
				result += frequency * (frequency - 1) / 2; 
			}
			return result;
		}
		
		List<PairOfInts> links1 = get(entity1).getLinks();
		List<PairOfInts> links2 = get(entity2).getLinks();
		int i = 0, j = 0, common = 0;
		for (; i < links1.size() && j < links2.size(); ++i, ++j) {
			int linkEntity1 = links1.get(i).getLeftElement();
			int linkEntity2 = links2.get(j).getLeftElement();
			if (linkEntity1 == linkEntity2) {
				common += links1.get(i).getRightElement() * links2.get(j).getRightElement(); 
				++i; 
				++j;
			} else if (linkEntity1 < linkEntity2) {
				++i;
			} else {
				++j;
			}
		}
		return common;
	}
	
	public long getConditionalDenominator(int entityCond, int entity) {
		long result = 0;
		for (PairOfInts link: get(entityCond).getLinks()) {
			int docno = link.getLeftElement();
			long entityCondFrequency = link.getRightElement();
			long docLinks;
			try {
			docLinks = get(docno).getNoLinks();
			} catch (Exception e) {
				throw new IllegalArgumentException("Cannot access:" + docno);
			}
			long diffPairs = entityCondFrequency * (docLinks - entityCondFrequency);
			long entityCondPairs = entityCondFrequency * (entityCondFrequency - 1) / 2;
			result += diffPairs + entityCondPairs;
		}
		return result;
	}
}
