package knowledgebase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class WikipediaAnchorTextIndex extends HashSet<String>{
	private static final long serialVersionUID = -2835232426707302378L;
	private static final int INITIAL_SIZE = 6423968;
	
	public WikipediaAnchorTextIndex(int size) {
	  // RAM (heap) efficient capacity setting
    super(size * 4 / 3 + 1);
	}
	
	public static WikipediaAnchorTextIndex load(String path)  throws IOException {
		WikipediaAnchorTextIndex dictionary = new WikipediaAnchorTextIndex(INITIAL_SIZE);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;

    while ((line = in.readLine()) != null ) {
        String[] elements = line.split("\t", 2);
        dictionary.add(elements[0]);
    }
    in.close();
    
    return dictionary;
  }
}
