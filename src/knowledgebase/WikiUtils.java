package knowledgebase;

import index.WikipediaRedirectPagesIndex;

public class WikiUtils {
	public static boolean isListPage(String title) {
		return title.startsWith("List of ") || title.startsWith("list of");
	}
	
	public static boolean isDisambiguationPage(String title) {
		return title.contains("(disambiguation)");
	}
	
	public static boolean isCategoryPage(String title) {
		return title.contains(":");
	}
	
	/*
	 * Several Wikipedia URLs can point to the same wikipedia page. 
	 */
	public static String getCanonicalURL(String title, WikipediaRedirectPagesIndex redirectIndex) {
		if (redirectIndex.containsKey(title)) {
			return redirectIndex.get(title);
		}
		return title;
	}
}
