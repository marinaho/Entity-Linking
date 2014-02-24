package knowledgebase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import normalizer.Normalizer;

public class WikipediaRedirectPagesIndex extends HashMap<String, String> {
	private static final long serialVersionUID = -1133700620016131767L;
	private static final int INITIAL_SIZE = 6423968;

	public WikipediaRedirectPagesIndex(int size) {
	  // RAM (heap) efficient capacity setting
    super(size * 4 / 3 + 1);
	}
	
	public static WikipediaRedirectPagesIndex load(String path)  throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(path));
		WikipediaRedirectPagesIndex map = new WikipediaRedirectPagesIndex(INITIAL_SIZE);
		String line;

    while ((line = in.readLine()) != null ) {
        String[] elements = line.split("\t", 3);
        map.put(elements[0].trim(), Normalizer.capitalizeFirstLetter(elements[1].trim()));
    }
    in.close();
    return map;
  }
}
