package index;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class TitleIDsIndex extends HashMap<Integer, String> {
	private static final long serialVersionUID = 7383601962396077148L;

	private static final int INITIAL_SIZE = 4399390;
	
	// Wikipedia titles not found in the index are disambiguation/list/category pages.
	public static final int NOT_CANONICAL_TITLE = -1;

	public TitleIDsIndex(int size) {
    super(size);
	}
	
	public static TitleIDsIndex load(String path)  throws IOException {
		// Set size to sustain all elements with a load factor of 0.75 
		TitleIDsIndex map = new TitleIDsIndex(INITIAL_SIZE * 4 / 3 + 1);
		
		FileReader fileReader = new FileReader(path);
		BufferedReader in = new BufferedReader(fileReader);
		String line;

    while ((line = in.readLine()) != null ) {
        String[] elements = line.split("\t", 3);
        map.put(Integer.parseInt(elements[1]), elements[0].trim());
    }
    in.close();
    fileReader.close();
    return map;
  }
}
