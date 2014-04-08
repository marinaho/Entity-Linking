package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class TermDocumentFrequencyIndex extends HashMap<String, Integer> {
	private static final long serialVersionUID = -4306252883493356345L;
	private static final int INITIAL_SIZE = 19886746;
	
	public TermDocumentFrequencyIndex(int size) {
    super(size);
	}
	
	public static TermDocumentFrequencyIndex load(String path)  throws IOException {
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		TermDocumentFrequencyIndex map = new TermDocumentFrequencyIndex(INITIAL_SIZE);
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
}
