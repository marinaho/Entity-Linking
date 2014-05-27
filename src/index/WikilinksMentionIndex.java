package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import normalizer.Normalizer;

public class WikilinksMentionIndex extends MentionIndex {
	private static final long serialVersionUID = -2807253325186264648L;
	private static final int INITIAL_SIZE = 9246146;
	
	public static final int MINIMUM_COUNT = 5;
	
	public WikilinksMentionIndex(int size) {
    super(size);
	}
	
	public static WikilinksMentionIndex load(String path)  throws IOException {
		WikilinksMentionIndex dictionary = new WikilinksMentionIndex(INITIAL_SIZE * 4 / 3 + 1);
		BufferedReader in = new BufferedReader(new FileReader(path));
		String line;
		final int NUMERATOR = 100000;
		
    while ((line = in.readLine()) != null ) {
    	if (!line.contains("\t") || !line.contains(";weightedByDictScore ")) {
    		continue;
    	}
      String[] elements = line.split("\t", 2);
      String mention = Normalizer.normalize(elements[0]);
      Double score = extractScore(elements[1]);
      
      dictionary.put(mention, new Integer[]{(int) (score * NUMERATOR), NUMERATOR});
    }

    in.close();
 
    return dictionary;
	}
	
	public static double extractScore(String context) {
		String startMarker = ";weightedByDictScore ";
    int beginIndex = context.indexOf(startMarker) + startMarker.length();
    String endMarker = " ;weightedByNumEntsScore";
    int endIndex = context.indexOf(endMarker);
    		
    return Double.parseDouble(context.substring(beginIndex, endIndex));
	}
}
