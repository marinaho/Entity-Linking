package md;

import index.CandidatesIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import data.TFIDFEntry;
import knowledgebase.WikiUtils;

public class MentionDetection {
	public static final int NGRAM_SIZE = 11;
	// Mentions with more than MAX_CANDIDATES will not be considered.
	private static final int MAX_CANDIDATES = 2000;
	// &nbsp occuring in HTML pages gets read as this value. This interferes in tokenization.
	public static final char NBSP = 160;
	public static final String DELIMITERS = " \t\n\r\f—/*`\"'()[]{},‘’“”;?_" + NBSP;
	public static final String DELIMITERS_KEEP = ".!-:";
	
	public static boolean skipManyCandidates = false;
	
	private double percent = 0.06;
	private boolean useThreshold = false;
	
	private boolean skipZeroLocalCompatibility = false;
	
	private String text;
	private CandidatesIndex mentionIndex;
	private EntityTFIDFIndex entityTFIDFIndex;
	private TermDocumentFrequencyIndex dfIndex;
		
	public MentionDetection(String text, CandidatesIndex mentionIndex, 
			EntityTFIDFIndex entityTFIDFIndex, TermDocumentFrequencyIndex dfIndex) {
		this.text = text;
		this.mentionIndex = mentionIndex;
		this.entityTFIDFIndex = entityTFIDFIndex;
		this.dfIndex = dfIndex;
	}
	
	public void setSkipZeroLocalCompatibility() {
		skipZeroLocalCompatibility = true;
	}
	
	public List<Mention> solve() throws IOException {
		List<Token> tokens = tokenizeText(text);
		List<Ngram> ngrams = gatherNgrams(tokens, NGRAM_SIZE);
		List<Mention> nameMentions = extractMentions(ngrams, tokens);
		nameMentions = computeLocalCompatibilities(nameMentions, tokens);
		computeImportance(nameMentions);
		return nameMentions;
	}
	
	/** Returns the extracted mentions and candidate entities. Local compatibilities are set to 0
	 *  and importance scores are not computed.
	 * @param percentMentions
	 * @return
	 * @throws IOException
	 */
	public List<Mention> getCandidateMentions(double percentMentions) 
			throws IOException {
		List<Token> tokens = tokenizeText(text);
		List<Ngram> ngrams = gatherNgrams(tokens, NGRAM_SIZE);
		List<Mention> result = extractMentions(ngrams, tokens);
		for (Mention mention: result) {
			for (Integer entity: mentionIndex.getCandidateEntities(mention)) {
				mention.setEntityCompatibilityScore(entity, BigDecimal.ZERO);
			}
		}
		return result;
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

		for (Ngram ngram: result) {
			String tokenSpan = text.substring(ngram.getOffset(), ngram.getOffset() + ngram.getLength());
			ngram.setOriginalNgram(tokenSpan);
		}
		return result;
	}
	
	
	/*
	 * Searches the ngrams in the mention index. Ranks them by keyphraseness and returns the top
	 * scoring x% as a map of (mention, candidate entities array).
	 */
	public List<Mention> extractMentions(List<Ngram> ngrams, List<Token> tokens) throws IOException {
		TreeSet<Mention> keyphrasenessTree = new TreeSet<Mention>(); 
		
		for (Ngram ngram: ngrams) {
			String normalizedNgram = ngram.getNgram();
			if (normalizedNgram.contains(" - ") && !mentionIndex.containsKey(normalizedNgram)) {
				normalizedNgram = normalizedNgram.replaceAll(" - ", " ");
				ngram.setNgram(normalizedNgram);
			}
			
			if (mentionIndex.containsKey(normalizedNgram)) {
				int candidatesCount = mentionIndex.getCandidateEntitiesCount(normalizedNgram);
				if (skipManyCandidates && candidatesCount > MAX_CANDIDATES) {
					System.out.println("Skipped " + normalizedNgram + " candidates " + candidatesCount);
					continue;
				}
				Mention mention = new Mention(ngram);
				mention.computeKeyphrasenessAndDF(mentionIndex);
				if (useThreshold) {
					rankByThreshold(mention, keyphrasenessTree);
				} else {
					int toExtract = Math.max((int)(tokens.size() * percent), 1);
					rankMention(mention, keyphrasenessTree, toExtract);
				}
			} 
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
	
	public void rankByThreshold(Mention mention, TreeSet<Mention> tree) {
		if (mention.getKeyphraseness() >= percent) {
			tree.add(mention);
		}
	}	
	
	public List<Mention>	computeLocalCompatibilities(List<Mention> mentions, List<Token> tokens) {
		List<Mention> result = new ArrayList<Mention>();
		for (Mention mention: mentions) {
			for (Integer entity: mentionIndex.getCandidateEntities(mention)) {
				BigDecimal score = getLocalMentionEntityCompatibility(mention, entity, tokens);
				mention.setEntityCompatibilityScore(entity, score);
			}
			if (skipZeroLocalCompatibility && 
					mention.computeSumCompatibilities().compareTo(BigDecimal.ZERO) == 0) {
				continue;
			}
			result.add(mention);
		}
		return result;
	}
	
	public BigDecimal getLocalMentionEntityCompatibility(Mention mention, int entity, 
			List<Token> tokens) {
		List<String> context = mention.extractContext(tokens);
		TFIDFEntry tfidfContext = getTFIDFContext(context);
		TFIDFEntry tfidfEntity = entityTFIDFIndex.getEntityTFIDFVector(entity);
		return cosineDistance(tfidfContext, tfidfEntity);
	}
		
	public TFIDFEntry getTFIDFContext(List<String> context) {
		Map<String, Integer> tfContext = getTFContext(context);
		TFIDFEntry result = new TFIDFEntry();
		for (Map.Entry<String, Integer> entry: tfContext.entrySet()) {
			String term = entry.getKey();
			Integer tf = entry.getValue();
			result.put(term, tf * dfIndex.getIDF(term));
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
	
	public BigDecimal cosineDistance(Map<String, Double> v1, Map<String, Double> v2) {

		BigDecimal numerator = new BigDecimal(0);
		for (String word: v1.keySet()) {
			if (v2.containsKey(word)) {
				numerator = numerator.add(
						new BigDecimal(v1.get(word))
								.multiply(new BigDecimal(v2.get(word))
						)
				);
			}
		}
		
		return numerator
				.divide(new BigDecimal(getNorm(v1.values())), RoundingMode.HALF_UP)
				.divide(new BigDecimal(getNorm(v2.values())), RoundingMode.HALF_UP);
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
		double sum = 0;
		for (int pos = 0; pos < mentions.size(); ++pos) {
			Mention mention = mentions.get(pos);
			int tf = countOccurences(mention.getOriginalNgram(), text); 
			double idf = Math.log((double) WikiUtils.WIKIPEDIA_DF_SIZE / mention.getDocumentFrequency());
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

	/**
	 * In case a threshold is set, the selected mentions will be the ones with keyphraseness >= 
	 * threshold. Otherwise the top percentMentions% by keyphraseness score in each document will
	 * be linked.
	 */
	public void setThreshold(double value, boolean useThreshold) {
		this.useThreshold = useThreshold;
		this.percent = value;
	}
}