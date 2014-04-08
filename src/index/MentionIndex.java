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
public class MentionIndex extends HashMap<String, Integer[]>{

	private static final long serialVersionUID = 8891794436477241276L;
	private static final int INITIAL_SIZE = 9112827;
	
	public static final int MINIMUM_COUNT = 5;
	
	public MentionIndex(int size) {
    super(size);
	}
	
	public static MentionIndex load(String path)  throws IOException {
		MentionIndex dictionary = new MentionIndex(INITIAL_SIZE);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t");
      
      // Skip mentions contained in less than MINIMUM_COUNT docs.
      int documentsCount = Integer.parseInt(elements[2]);
      if (documentsCount < MINIMUM_COUNT) {
      	continue;
      }
      
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
		return Arrays.copyOfRange(value, 2, value.length);
	}
}
