package normalizer;

import java.net.URI;
import java.net.URISyntaxException;
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
	
	public static String removeTagsAndProps(String input, List<String> tags) {
		for (String tag: tags) {
			input = input.replaceAll("<" + tag + "([^>]*)" + ">([^<]*)</" + tag + ">", "$2");
		}
		return input;
	}
	
	public static String normalize(String input) {
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = StringEscapeUtils.unescapeXml(input);
		input = input.toLowerCase();
		input = input.replaceAll("<!--([^-]*)-->" , ""); // Remove HTML comments.
		input = removeChars(input, "\"'()[]{},‘’“”."); // Remove each occurrence of these characters.
		input = removeTags(input, Arrays.asList("big", "small", "code", "del", "math", "noinclude", 
				"nowiki", "ref", "s", "sub", "sup", "tt", "u", "var"));
		input = removeTagsAndProps(input, Arrays.asList("abbr", "span"));
		input = input.replace("-", " ");			// Replace dashes with space.
		input = input.replace("—", " ");
		input = input.replace("\t", " ");
		input = input.replace("<br>", " ");
		input = input.replace("<br />", " ");
		
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
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = StringEscapeUtils.unescapeXml(input);
		input = input.replaceAll("<!--([^-]*)-->" , ""); // Remove HTML comments.
		input = removeTags(input, Arrays.asList("ref"));
		input = input.replace("_", " ").trim();
		input = capitalizeFirstLetter(input);
		String decodedInput;
		try {
			decodedInput = new URI("http://a.a/" + input).getPath().substring(1);
		} catch (URISyntaxException e) {
			return input;
		}
		return decodedInput;
	}
	
	public static String removeHashFromLink(String input) {
		if (input.indexOf('#') != -1) {
			return input.substring(0, input.indexOf('#'));
		}
		return input;
	}
	
	public static String capitalizeFirstLetter(String original){
    if(original.length() == 0)
        return original;
    return original.substring(0, 1).toUpperCase() + original.substring(1);
	}
}
