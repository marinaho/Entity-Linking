package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Maps Wikipedia page titles to integer ids.
 * For computation of index file @see knowledgebase.WikiArticleTitlesIndexBuilder
 */
public class TitlesIndex extends HashMap<String, Integer> {
	private static final long serialVersionUID = -2569377647680616197L;
	private static final int INITIAL_SIZE = 4399390;
	
	// Wikipedia titles not found in the index are disambiguation/list/category pages.
	public static final int NOT_CANONICAL_TITLE = -1;

	public TitlesIndex(int size) {
    super(size);
	}
	
	public static TitlesIndex load(String path)  throws IOException {
		// Set size to sustain all elements with a load factor of 0.75 
		TitlesIndex map = new TitlesIndex(INITIAL_SIZE * 4 / 3 + 1);
		
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		String line;

    while ((line = in.readLine()) != null ) {
        String[] elements = line.split("\t", 3);
        map.put(elements[0].trim(), Integer.parseInt(elements[1]));
    }
    in.close();
    fileReader.close();
    return map;
  }
	
	public int getTitleId(String title) {
		if (super.containsKey(title)) {
				return super.get(title);
		}
		return NOT_CANONICAL_TITLE;
	}
}
