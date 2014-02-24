package knowledgebase;

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
	
	public static String redirect(String title, WikipediaRedirectPagesIndex redirectIndex) {
		if (redirectIndex.containsKey(title)) {
			return redirectIndex.get(title);
		}
		return title;
	}
	
	public static int getTitleId(String title, WikipediaTitlesIndex index) {
		if (index.containsKey(title)) {
			return index.get(title);
		}
		return -1;
	}
}
