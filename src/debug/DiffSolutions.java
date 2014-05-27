package debug;

import iitb.NameAnnotation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

public class DiffSolutions {
	/**
	 *  Reads two sets of output annotations. Compares the ones classified wrong by the first 
	 *  classifier with the output of the second.
	 * @throws FileNotFoundException 
	 */
	
	public static final String FILE_MARKER = " Precision:";
	public static final String FP_MARKER = "False positives:";
	public static final String FN_MARKER = "False negatives:";
	public static final String GOOD_MARKER = "Good";
	public static final String ENTITY_MARKER = "Entity:";
	public static final String ENTITY_FINISH_MARKER = " File:";
	public static final String NAME_MARKER = "Name:";
	public static final String NAME_FINISH_MARKER = " Entity id:";
	
	public static void main(String args[]) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage: [output file EL 1] [output file EL 2]");
			return;
		}
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		String line, filename = "";
		boolean isFP = false, isFN = false, isGood = false;
		
		Set<String> names = null;
		HashMap<String, String> misclassified = new HashMap<String, String>();
		while((line = in.readLine()) != null) {
			if (line.contains(FILE_MARKER)) {
				filename = line.split(" ")[0];
				names = new HashSet<String>();
			} else if (line.equals(GOOD_MARKER)) {
				isGood = true;
				isFP = false;
				isFN = false;
			} else if (line.equals(FP_MARKER)) {
				isFP = true;
				isFN = false;
				isGood = false;
			} else if (line.equals(FN_MARKER)) {
				isFN = true;
				isFP = false;
				isGood = false;
			} else if (StringUtils.isEmpty(line)) {
				isFN = false;
				isFP = false;
				isGood = false;
			} else if (isFP) {
				names.add(extractName(line));
			} else if (isFN) {
				String name = extractName(line);
				if (names.contains(name)) {
					String entity = extractEntity(line);
					misclassified.put(filename + " " + name, entity);
				}
			}
		}
		
		in = new BufferedReader(new FileReader(args[1]));
		int good = 0;
		int wrong = 0;
		while((line = in.readLine()) != null) {
			if (line.contains(FILE_MARKER)) {
				filename = line.split(" ")[0];
			} else if (line.equals(GOOD_MARKER)) {
				isGood = true;
				isFP = false;
				isFN = false;
			} else if (line.equals(FP_MARKER)) {
				isFP = true;
				isFN = false;
				isGood = false;
			} else if (line.equals(FN_MARKER)) {
				isFN = true;
				isFP = false;
				isGood = false;
			} else if (StringUtils.isEmpty(line)) {
				isFN = false;
				isFP = false;
				isGood = false;
			} else if (isFP) {
				String name = extractName(line);
				String index = filename + " " + name;
				if (misclassified.containsKey(index)) {
					String entity = extractEntity(line);
					System.out.println("Wrong:" + index + " Entity:" + entity);
					++wrong;
				}
			} else if (isGood) {
				String name = extractName(line);
				String index = filename + " " + name;
				if (misclassified.containsKey(index)) {
					String entity = extractEntity(line);
					System.out.println("Good:" + index + " Entity:" + entity);
					++good;
				}
			}
		}
		System.out.println("Wrong:" + wrong + " Good:" + good + " Total:" + misclassified.size());
	}
	
	private static String extractName(String line) {
		int startPos = line.indexOf(NAME_MARKER) + NAME_MARKER.length();
		int endPos = line.indexOf(NAME_FINISH_MARKER);
		return line.substring(startPos, endPos);
	}
	
	private static String extractEntity(String line) {
		int startPos = line.indexOf(ENTITY_MARKER) + ENTITY_MARKER.length();
		int endPos = line.indexOf(ENTITY_FINISH_MARKER);
		return line.substring(startPos, endPos);
	}
}
