package md;

import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import knowledgebase.WikiUtils;
import edu.umd.cloud9.io.pair.PairOfStringFloat;

public class MentionDetection {
	public static final int NGRAM_SIZE = 11;
	// &nbsp occuring in HTML pages gets read as this value. This interferes in tokenization.
	public static final char NBSP = 160;
	public static final String DELIMITERS = " \t\n\r\f—/*`\"'()[]{},‘’“”;?_" + NBSP;
	public static final String DELIMITERS_KEEP = ".!-:";
	
	// Mentions are linked if they score in top x% keyphraseness score.
	private double percentMentions = 0.06;
	
	private String text;
	private MentionIndex mentionIndex;
	private EntityTFIDFIndex entityTFIDFIndex;
	private TermDocumentFrequencyIndex dfIndex;
		
	public MentionDetection(String text, MentionIndex mentionIndex, EntityTFIDFIndex entityTFIDFIndex, 
			TermDocumentFrequencyIndex dfIndex) {
		this.text = text;
		this.mentionIndex = mentionIndex;
		this.entityTFIDFIndex = entityTFIDFIndex;
		this.dfIndex = dfIndex;
	}
	
	public List<Mention> solve() throws IOException {
		List<Token> tokens = tokenizeText(text);
		List<Ngram> ngrams = gatherNgrams(tokens, NGRAM_SIZE);
		int toExtract = (int)(tokens.size() * percentMentions + 1);
		List<Mention> nameMentions = extractMentions(ngrams, toExtract);
		computeMentionEntityCompatibilities(nameMentions, tokens);
		computeImportance(nameMentions);
		return nameMentions;
	}
	
	public static List<Token> tokenizeText(String text) {
		String delimiters = DELIMITERS + DELIMITERS_KEEP;
		ArrayList<Token> result = new ArrayList<Token>();
		StringBuilder token = new StringBuilder("");
		int offset = -1;
		
		// Enforce text ends with a delimiter.
		text = text + " ";
		for(int i = 0; i < text.length(); ++i) {
			char ch = text.charAt(i);
			if (delimiters.indexOf(ch) == -1) {
				if (token.length() == 0) {
					// Start new token.
					offset = i;
				}
				// Append character.
				token.append(ch);
			} else {
				// Add token to results if it is not empty.
				if (token.length() > 0) {
					result.add(new Token(token.toString(), offset));
				}
				
				// If delimiter is '.' or '!' etc, add a token for it.
				if (DELIMITERS_KEEP.indexOf(ch) != -1) {
					result.add(new Token("" + ch, i));
				}
				
				// Start new token.
				token = new StringBuilder("");
			}
		}
		return result;
	}
	
	public ArrayList<Ngram> gatherNgrams(List<Token> tokens, int maxSize) {
		if (maxSize < 0) {
			throw new IllegalArgumentException();
		}

		ArrayList<Ngram> result = new ArrayList<Ngram>();
		Ngram ngrams[] = new Ngram[maxSize];
		int i, j;
		
		// Create ngrams with tokens in 0 ... maxSize-1
		for (i = 0; i < tokens.size() && i < maxSize; ++i) {
			Token token = tokens.get(i);
			ngrams[i] = new Ngram(token);
			result.add(ngrams[i].copy());
			
			// Create ngrams ending in token[i]
			for(j = 0; j < i; ++j) {
				ngrams[j].append(token);
				// Add ngram token[j], token[j+1], ... token[i]
				result.add(ngrams[j].copy());
			}
		}
		
		int start = 0;
		for (; i < tokens.size(); ++i) {
			Token token = tokens.get(i);
			ngrams[start] = new Ngram(token);
			result.add(ngrams[start].copy());
			
			// Create ngrams ending in token[i].
			for (j = (start + 1) % maxSize; j != start; j = (j + 1) % maxSize) {
				ngrams[j].append(token);
				result.add(ngrams[j].copy());
			}
			start = (start + 1) % maxSize;
		}

		
		return result;
	}
	
	
	/*
	 * Searches the ngrams in the mention index. Ranks them by keyphraseness and returns the top
	 * scoring x% as a map of (mention, candidate entities array).
	 */
	public List<Mention> extractMentions(List<Ngram> ngrams, int toExtract) throws IOException {
		TreeSet<Mention> keyphrasenessTree = new TreeSet<Mention>(); 
		
		for (Ngram inputNgram: ngrams) {
			String ngram = inputNgram.getNgram();
			if (ngram.contains("-") && !mentionIndex.containsKey(ngram)) {
				ngram = ngram.replaceAll(" - ", " ");
			}
			if (mentionIndex.containsKey(ngram)) {
				Mention mention = new Mention(ngram, inputNgram.getOffset(), inputNgram.getLength());
				mention.computeKeyphrasenessAndDF(mentionIndex);
				rankMention(mention, keyphrasenessTree, toExtract);
			} 
		}
		
		for (Mention mention: keyphrasenessTree) {
			mention.setCandidateEntities(mentionIndex);
		}
		
		return new ArrayList<Mention>(keyphrasenessTree);
	}
	
	/*
	 *  If the mention is in the top scoring set, it is added to the tree. At most toExtract elements
	 *  are added in the tree. 
	 */
	public static void rankMention(Mention mention, TreeSet<Mention> tree, int toExtract) {
		if (tree.size() < toExtract) {
			tree.add(mention);
		} else {
			if (tree.first().getKeyphraseness() < mention.getKeyphraseness()) {
				tree.remove(tree.first());
				tree.add(mention);
			}
		}
	}	
	
	public void computeMentionEntityCompatibilities(List<Mention> mentions, List<Token> tokens) 
			throws IOException {
		for (Mention mention: mentions) {
			Integer[] candidates = mention.getCandidateEntities();
			Double scores[] = new Double[candidates.length];
			
			for (int i = 0; i < candidates.length; ++i) {
				int candidate = candidates[i];
				List<String> context = mention.extractContext(tokens);
				scores[i] = getLocalMentionEntityCompatibility(context, candidate);
			}		
			mention.setEntityCompatibilityScores(scores);
		}
	}
	
	public double getLocalMentionEntityCompatibility(List<String> context, int entity) {
		Map<String, Double> tfidfContext = getTFIDFContext(context);
		Map<String, Double> tfidfEntity = getTFIDFEntity(entity);
		return cosineDistance(tfidfContext, tfidfEntity);
	}
		
	public Map<String, Double> getTFIDFContext(List<String> context) {
		Map<String, Integer> tfContext = getTFContext(context);
		Map<String, Double> result = new HashMap<String, Double>();
		for (Map.Entry<String, Integer> entry: tfContext.entrySet()) {
			String term = entry.getKey();
			Integer tf = entry.getValue();
			result.put(term, tf * getIDF(term, dfIndex));
		}
		return result;
	}
	
	public Map<String, Integer> getTFContext(List<String> context) {
		Map<String, Integer> result = new HashMap<String, Integer>();
		for (String word: context) {
			if (result.containsKey(word)) {
				result.put(word, result.get(word) + 1);
			} else {
				result.put(word, 1);
			}
		}
		return result;
	}

	public static double getIDF(String mention, TermDocumentFrequencyIndex dfIndex) {
		return Math.log((double) WikiUtils.WIKIPEDIA_SIZE / getDF(mention, dfIndex));
	}
	
	public static int getDF(String term, TermDocumentFrequencyIndex dfIndex) {
		return dfIndex.get(term);
	}
	
	public Map<String, Double> getTFIDFEntity(int entity) {
		Map<String, Double> result = new HashMap<String, Double>();
		List<PairOfStringFloat> tfIDFEntity = entityTFIDFIndex.getEntityTFIDFVector(entity);
		for(PairOfStringFloat entry: tfIDFEntity) {
			result.put(entry.getLeftElement(), (double) entry.getRightElement());
		}
		return result;
	}
	
	public double cosineDistance(Map<String, Double> v1, Map<String, Double> v2) {

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
	
	public void computeImportance(List<Mention> mentions) {
		Double[] tfidf = new Double[mentions.size()];
		int sum = 0;
		for (int pos = 0; pos < mentions.size(); ++pos) {
			Mention mention = mentions.get(pos);
			int tf = countOccurences(mention.getNgram(), text); 
			double idf = Math.log((double) WikiUtils.WIKIPEDIA_SIZE / mention.getDocumentFrequency());
			tfidf[pos] = tf * idf;
			sum += tfidf[pos];
		}
		for (int pos = 0; pos < mentions.size(); ++pos) {
			Mention mention = mentions.get(pos);
			mention.setImportance(tfidf[pos] / sum);
		}
	}
	
	public static int countOccurences(String substring, String text) {
		int result = 0, pos = 0;
		while ((pos = text.indexOf(substring, pos)) != -1) {
			++result;
			pos += text.length();
		}
		return result;
	}
	
	public void setPercentMentionsToExtract(double percent) {
		percentMentions = percent;
	}
}
