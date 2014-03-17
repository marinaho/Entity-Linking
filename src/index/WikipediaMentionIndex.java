package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import edu.umd.cloud9.io.pair.PairOfInts;

/*
 * Maps mentions to entity list.
 */
public class WikipediaMentionIndex extends HashMap<String, Integer[]>{

	private static final long serialVersionUID = 8891794436477241276L;
	private static final int INITIAL_SIZE = 6423968;
	
	public WikipediaMentionIndex(int size) {
    super(size * 4 / 3 + 1);
	}
	
	public static WikipediaMentionIndex load(String path)  throws IOException {
		WikipediaMentionIndex dictionary = new WikipediaMentionIndex(INITIAL_SIZE);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t");
      Integer[] mapValue = new Integer[elements.length - 1];
      for (int i = 1; i < elements.length; ++i) {
      	mapValue[i - 1] = Integer.parseInt(elements[i]);
      }
      dictionary.put(elements[0], mapValue);
    }
    in.close();
    
    return dictionary;
  }
	
	public PairOfInts getKeyphraseness(String key) {
		Integer[] value = get(key);
		return new PairOfInts(value[0], value[1]);
	}
	
	public Integer[] getCandidateEntities(String key) {
		Integer[] value = get(key);
		return Arrays.copyOfRange(value, 2, value.length - 1);
	}
}
