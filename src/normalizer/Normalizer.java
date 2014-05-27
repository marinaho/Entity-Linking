package normalizer;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;

import md.MentionDetection;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;

public class Normalizer {	
	public static String WHITESPACES = " \t\n\r\f";
	
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
	
	public static String normalizeMention(String input) {
		return input;
	}
	
	public static String normalize(String input) {
		// Unescape HTML and XML.
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = StringEscapeUtils.unescapeXml(input);
		
		input = input.toLowerCase();
		
		// Remove HTML boilerplate.
		input = input.replaceAll("<!--([^-]*)-->" , ""); 
		input = removeTags(input, Arrays.asList("big", "small", "code", "del", "math", "noinclude", 
				"nowiki", "ref", "s", "sub", "sup", "tt", "u", "var"));
		input = removeTagsAndProps(input, Arrays.asList("abbr", "span"));
		input = input.replace("<br>", " ");
		input = input.replace("<br />", " ");
		
		for (int i = 0; i < MentionDetection.DELIMITERS.length(); ++i) {
			char delimiter = MentionDetection.DELIMITERS.charAt(i);
			input = input.replace("" + delimiter, " ");
		}
		
		for (int i = 0; i < MentionDetection.DELIMITERS_KEEP.length(); ++i) {
			char delimiterKeep = MentionDetection.DELIMITERS_KEEP.charAt(i);
			input = input.replace("" + delimiterKeep, " " + delimiterKeep + " ");
		}
		// Remove consecutive spaces.
		return Joiner.on(" ").join(StringUtils.split(input, " \t\n\r\f"));
	}
		
	// Same as normalize but we do not want to keep delimiters.
	public static String normalizeNoDelimiters(String input) {
		// Unescape HTML and XML.
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = StringEscapeUtils.unescapeXml(input);
		
		input = input.toLowerCase();
		
		// Remove HTML boilerplate.
		input = input.replaceAll("<!--([^-]*)-->" , ""); 
		input = removeTags(input, Arrays.asList("big", "small", "code", "del", "math", "noinclude", 
				"nowiki", "ref", "s", "sub", "sup", "tt", "u", "var"));
		input = removeTagsAndProps(input, Arrays.asList("abbr", "span"));
		input = input.replace("<br>", " ");
		input = input.replace("<br />", " ");
		
		for (int i = 0; i < MentionDetection.DELIMITERS.length(); ++i) {
			char delimiter = MentionDetection.DELIMITERS.charAt(i);
			input = input.replace("" + delimiter, " ");
		}
		
		for (int i = 0; i < MentionDetection.DELIMITERS_KEEP.length(); ++i) {
			char delimiter = MentionDetection.DELIMITERS_KEEP.charAt(i);
			input = input.replace("" + delimiter, " ");
		}
		// Remove consecutive spaces.
		return Joiner.on(" "). join(StringUtils.split(input, " \f\n\t\r"));
			
	}
	
	public static String processAnchorText(String input) {
		return normalize(input);
	}
	
	public static String processTargetLink(String input) throws UnsupportedEncodingException {
		input = StringEscapeUtils.unescapeXml(input);
		input = StringEscapeUtils.unescapeHtml4(StringEscapeUtils.unescapeHtml4(input));
		input = StringEscapeUtils.unescapeXml(input);
		input = input.replaceAll("<!--([^-]*)-->" , ""); // Remove HTML comments.
		input = removeTags(input, Arrays.asList("ref"));
		input = input.replace("_", " ").trim();
		input = capitalizeFirstLetter(input);
		
		if (!input.contains("%")) {
			return input;
		}
		
		String decodedInput;
		try {
			decodedInput = URLDecoder.decode("http://a.a/" + input, "UTF-8").substring(11);
		} catch (IllegalArgumentException e) {
			return input.trim();
		}
		return decodedInput.trim();
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
