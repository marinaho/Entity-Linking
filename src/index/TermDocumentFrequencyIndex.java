package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import knowledgebase.WikiUtils;

/**
 * Maps a term to it's document frequency.
 * The file loaded should be produced by @see knowledgebase.EntityMentionIndexBuilder .
 */
public class TermDocumentFrequencyIndex extends HashMap<String, Integer> {
	private static final long serialVersionUID = -4306252883493356345L;
	private static final int INITIAL_SIZE = 12229505;
	
	public TermDocumentFrequencyIndex(int size) {
    super(size);
	}
	
	public static TermDocumentFrequencyIndex load(String path)  throws IOException {
		TermDocumentFrequencyIndex map = new TermDocumentFrequencyIndex(INITIAL_SIZE * 4 / 3 + 1);
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		String line;

    while ((line = in.readLine()) != null ) {
      String[] elements = line.split("\t", 3);
      map.put(elements[0].trim(), Integer.parseInt(elements[1].trim()));
    }
    in.close();
    fileReader.close();
    return map;
  }
	
	@Override
	public Integer get(Object term) {
		if (containsKey(term)) {
			return super.get(term) + 1;
		}
		return 1;
	}
	
	public double getIDF(Object term) {
		int df = get(term);
		if (df == WikiUtils.WIKIPEDIA_DF_SIZE) {
			return 0.0;
		}
		return Math.log10((double) WikiUtils.WIKIPEDIA_DF_SIZE / df);
	}
}
