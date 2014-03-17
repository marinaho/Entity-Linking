package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/*
 * Maps Wikipedia redirect pages to canonical page title.
 */
public class WikipediaRedirectPagesIndex extends HashMap<String, String> {
	private static final long serialVersionUID = -1133700620016131767L;
	private static final int INITIAL_SIZE = 6423968;

	public WikipediaRedirectPagesIndex(int size) {
    super(size * 4 / 3 + 1);
	}
	
	public static WikipediaRedirectPagesIndex load(String path)  throws IOException {
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		WikipediaRedirectPagesIndex map = new WikipediaRedirectPagesIndex(INITIAL_SIZE);
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t", 3);
      map.put(elements[0].trim(), elements[1].trim());
    }
    in.close();
    fileReader.close();
    return map;
  }
}
