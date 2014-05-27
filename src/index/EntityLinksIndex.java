package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import knowledgebase.WikiUtils;

/**
 * Stores a mapping of (entity1, number of entities that link to the key).
 * Can be used to compute semantic relatedness between two entities.
 * The file loaded should be produced by @see knowledgebase.EntityMentionIndexBuilder .
 */
public class EntityLinksIndex extends HashMap<Integer, Integer[]> {
	// Don't change separator. 
	public static final String SEPARATOR = "\t";
	private static final long serialVersionUID = 2718316266900207592L;
	private static final int INITIAL_SIZE = 3796235;
	
	public EntityLinksIndex(int size) {
    super(size);
	}
	
	public static EntityLinksIndex load(String path)  throws IOException {
		EntityLinksIndex dictionary = new EntityLinksIndex(INITIAL_SIZE * 4 / 3 + 1);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
    	String[] tokens = line.split(SEPARATOR);
    	Integer[] mapValue = new Integer[tokens.length - 1];
    	for (int i = 1; i < tokens.length; ++i)
    		mapValue[i - 1] = Integer.parseInt(tokens[i]);
    	dictionary.put(Integer.parseInt(tokens[0]), mapValue);
    }
    in.close();
    
    return dictionary;
  }
	
	public double getSemanticRelatedness(int entity1, int entity2) {
		if (entity1 == entity2) {
			return 1.0;
		}
		Integer[] a = get(entity1);
		Integer[] b = get(entity2);
		int intersectSize = intersectSize(a, b);
		if (intersectSize == 0) {
			return 0.0;
		}
		
		return 1 - 
				(Math.log(Math.max(a.length, b.length)) - Math.log(intersectSize)) /
				(Math.log(WikiUtils.WIKIPEDIA_ARTICLES_SIZE) - Math.log(Math.min(a.length, b.length)));
	}
	
	public int getPopularity(int entity) {
		Integer[] inlinks = get(entity);
		return 1 + (inlinks != null ? inlinks.length : 0);
	}
	
	public int getCocitation(int entity1, int entity2) {
		if (entity1 == entity2) {
			return getPopularity(entity1);
		}
		return intersectSize(get(entity1), get(entity2));
	}
	
	public static int intersectSize(Integer[] a, Integer[] b) {
		if (a == null || b == null) {
			return 0;
		}
		int result = 0;
		for (int i = 0, j = 0; i < a.length && j < b.length;) {
			if (a[i].equals(b[j])) {
				++result; ++i; ++j;
			} else if (a[i] < b[j]) {
				++i;
			} else {
				++j;
			}
		}
		return result;
	}
}
