package knowledgebase;

/**
 * Utilities for working with Wikipedia.
 */
public class WikiUtils {
	public static final int WIKIPEDIA_DF_SIZE = 4532295;
	public static final int WIKIPEDIA_ARTICLES_SIZE = 4399390;
	
	public static boolean isListPage(String title) {
		return title.startsWith("List of ") || title.startsWith("list of");
	}
	
	public static boolean isDisambiguationPage(String title) {
		return title.contains("(disambiguation)");
	}
	
	public static boolean isCategoryPage(String title) {
		return title.contains(":");
	}
}
