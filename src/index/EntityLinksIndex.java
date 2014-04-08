package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class EntityLinksIndex extends HashMap<Integer, Integer[]>{

	private static final long serialVersionUID = 2718316266900207592L;
	private static final int INITIAL_SIZE = 3814359;
	
	public EntityLinksIndex(int size) {
    super(size);
	}
	
	public static EntityLinksIndex load(String path)  throws IOException {
		EntityLinksIndex dictionary = new EntityLinksIndex(INITIAL_SIZE);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t");
      Integer[] mapValue = new Integer[elements.length - 1];
      for (int i = 1; i < elements.length; ++i) {
      	mapValue[i - 1] = Integer.parseInt(elements[i]);
      }
      dictionary.put(Integer.parseInt(elements[0]), mapValue);
    }
    in.close();
    
    return dictionary;
  }
}
