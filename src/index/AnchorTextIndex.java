package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/**
 * Index containing all possible name mentions. Should load the file computed by 
 * @see knowledgebase.EntityMentionIndexBuilder
 */
public class AnchorTextIndex extends HashSet<String> {
	private static final long serialVersionUID = -2835232426707302378L;
	private static final int INITIAL_SIZE = 9202002;
	
	public AnchorTextIndex(int size) {
    super(size);
	}
	
	public static AnchorTextIndex load(String path)  throws IOException {
		AnchorTextIndex dictionary = new AnchorTextIndex(INITIAL_SIZE * 4 / 3 + 1);
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
