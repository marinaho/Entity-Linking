package baseline;

import index.WikipediaEntityLinksIndex;
import index.WikipediaMentionIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;

import normalizer.Normalizer;

public class RandomGraphWalk {
//	private static int NGRAMS_SIZE = 8;
	private static int WIKIPEDIA_SIZE = 4410555;
	private String normalizedText;
	WikipediaMentionIndex mentionEntityIndex;
	WikipediaEntityLinksIndex entityLinksIndex;
	
	public RandomGraphWalk(String inputText, String anchorTextIndexFilename, 
			String entityLinksFilename) throws IOException {
		normalizedText = Normalizer.normalize(inputText);
		mentionEntityIndex = WikipediaMentionIndex.load(anchorTextIndexFilename);
		entityLinksIndex = WikipediaEntityLinksIndex.load(entityLinksFilename);
	}
	
	public void solve() throws IOException {
//		List<String> ngrams = gatherNgrams(NGRAMS_SIZE);
//		List<String> nameMentions = extractMentions(ngrams); 
	}
	
	public ArrayList<String> gatherNgrams(int maxSize) {
		if (maxSize < 0) {
			throw new IllegalArgumentException();
		}
		
		StringTokenizer tokenizer = new StringTokenizer(normalizedText);
		StringBuilder ngrams[] = new StringBuilder[maxSize];
		
		ArrayList<String> result = new ArrayList<String>();
		for (int i = 0; i < maxSize && tokenizer.hasMoreTokens(); ++i) {
			ngrams[i] = new StringBuilder("");
			String next = tokenizer.nextToken();
			for(int j = 0; j <= i; ++j) {
				ngrams[j].append(next);
				result.add(ngrams[j].toString());
			}
		}
		
		int start = 0;
		while (tokenizer.hasMoreTokens()) {
			String next = tokenizer.nextToken();
			ngrams[start] = new StringBuilder(next);
			result.add(next);
			
			for (int i = (start + 1) % maxSize; i != start; i = (i + 1) % maxSize) {
				ngrams[i].append(next);
				result.add(ngrams[i].toString());
			}
		}
		return result;
	}
	
	public List<String> extractMentions(List<String> ngrams) throws IOException {
		List<String> result = new ArrayList<String>();

		for (String ngram: ngrams) {
			if (mentionEntityIndex.containsKey(ngram)) {
				result.add(ngram);
			}
		}
		return result;
	}
	
	public double getLocalMentionEntityCompatibility(List<String> context, int entity) {
		Map<String, Double> tfidfContext = getTFIDFContext(context);
		Map<String, Double> tfidfEntity = getTFIDFEntity(entity, tfidfContext);
		return cosineDistance(tfidfContext, tfidfEntity);
	}
		
	public Map<String, Double> getTFIDFContext(List<String> context) {
		Map<String, Double> result = new HashMap<String, Double>();
		for (String word: context) {
			result.put(word, getTFIDF(word));
		}
		return result;
	}

	public double getTFIDF(String mention) {
		return getTF(mention) * Math.log(WIKIPEDIA_SIZE / getDF(mention));
	}
	
	public double getTF(String mention) {
		 return StringUtils.countMatches(normalizedText, mention);
	}
	public double getDF(String term) {
		// TO DO
		return 0.0;
	}
	
	public Map<String, Double> getTFIDFEntity(int entity, Map<String, Double> tfidfContext) {
		// TO DO
		return new HashMap<String, Double>();
	}
	
	public double cosineDistance(Map<String, Double> v1, Map<String, Double> v2) {
		if (v2.size() < v1.size()) {
			return cosineDistance(v2, v1);
		}
		
		double numerator = 0;
		for (String word: v1.keySet()) {
			if (v2.containsKey(word)) {
				numerator += v1.get(word) * v2.get(word);
			}
		}
		
		return numerator / getNorm(v1.values()) / getNorm(v2.values());
	}
	
	public double getNorm(Collection<Double> v) {
		double res = 0;
		for (Double value: v) {
			res += value * value;
		}
		return Math.sqrt(res);
	}
	
	public double getSemanticRelatedness(int entity1, int entity2) {
		Integer[] a = entityLinksIndex.get(entity1);
		Integer[] b = entityLinksIndex.get(entity2);
		int intersectSize = intersectSize(a, b);
		return 1 - 
				(Math.log(Math.max(a.length, b.length)) - Math.log(intersectSize)) /
				(Math.log(WIKIPEDIA_SIZE) - Math.log(Math.min(a.length, b.length)));
	}
	
	public int intersectSize(Integer[] a, Integer[] b) {
		Set<Integer> set = new HashSet<Integer>();
		for (int element: a) {
			set.add(element);
		}
		
		int result = 0;
		for (int element: b) {
			if (set.contains(element)) {
				++result;
			}
		}
		return result;
	}
}
