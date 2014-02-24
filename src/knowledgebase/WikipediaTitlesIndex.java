package knowledgebase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class WikipediaTitlesIndex extends HashMap<String, Integer> {
	private static final long serialVersionUID = -2569377647680616197L;
	private static final int INITIAL_SIZE = 6423968;

	public WikipediaTitlesIndex(int size) {
	  // RAM (heap) efficient capacity setting
    super(size * 4 / 3 + 1);
	}
	
	public static WikipediaTitlesIndex load(String path)  throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(path));
		WikipediaTitlesIndex map = new WikipediaTitlesIndex(INITIAL_SIZE);
		String line;

    while ((line = in.readLine()) != null ) {
        String[] elements = line.split("\t", 3);
        map.put(elements[0].trim(), Integer.parseInt(elements[1]));
    }
    in.close();
    return map;
  }
}
