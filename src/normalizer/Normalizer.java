package normalizer;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringEscapeUtils;

public class Normalizer {
	public static String removeChars(String input, String toRemove) {
		for (int i = 0; i < toRemove.length(); ++i) {
			input = input.replace(String.valueOf(toRemove.charAt(i)), "");
		}
		return input;
	}
	
	public static String removeTags(String input, List<String> tags) {
		for (String tag: tags) {
			input = input.replaceAll("<" + tag + ">([^<]*)</" + tag + ">", "$1");
		}
		return input;
	}
	
	public static String normalize(String input) {
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = removeTags(input, Arrays.asList("big", "small", "code", "sub", "sup", "math", 
				"nowiki"));
		input = input.replace("-", " ");			// Replace dashes with space.
		input = input.replace("—", " ");
		input = input.replace("\t", " ");
		input = input.replace("<br />", " ");
		input = removeChars(input, "\"'()[]{},‘’“”."); // Remove each occurence of the characters.
		input = input.toLowerCase();
		
		StringTokenizer tokenizer = new StringTokenizer(input);
		StringBuilder output = new StringBuilder();
		
		while(tokenizer.hasMoreTokens()) {
			output.append(tokenizer.nextToken().trim() + " ");
		}
		
		return output.toString().trim();
	}
	
	public static String processAnchorText(String input) {
		return normalize(input);
	}
	
	public static String processTargetLink(String input) {
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = input.replace("_", " ").trim();
		return capitalizeFirstLetter(input);
	}
	
	public static String capitalizeFirstLetter(String original){
    if(original.length() == 0)
        return original;
    return original.substring(0, 1).toUpperCase() + original.substring(1);
	}
}
