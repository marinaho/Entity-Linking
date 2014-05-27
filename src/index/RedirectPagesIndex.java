package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * Maps Wikipedia redirect pages to canonical page title.
 * The loaded file should be produced by @see knowledgebase.PreprocessRedirectMapping .
 */
public class RedirectPagesIndex extends HashMap<String, String> {
	private static final long serialVersionUID = -1133700620016131767L;
	private static final int INITIAL_SIZE = 6423968;

	public RedirectPagesIndex(int size) {
    super(size);
	}
	
	public static RedirectPagesIndex load(String path)  throws IOException {
		RedirectPagesIndex map = new RedirectPagesIndex(INITIAL_SIZE * 4 / 3 + 1);
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t", 3);
      String fromTitle = elements[0].trim();
      String toTitle = elements[1].trim();
      map.put(fromTitle, toTitle);
    }
    in.close();
    fileReader.close();
    return map;
  }
	
	/*
	 * Redirects the input to the canonical URL, or leaves it unchanged, in case it is not a redirect
	 * URL.
	 */
	public String getCanonicalURL(String input) {
		if (containsKey(input)) {
			return get(input);
		}
		return input;
	}
}
