package md;

import iitb.IITBDataset;
import index.CandidatesIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umd.cloud9.io.pair.PairOfInts;

/*
 * Encapsulates an ngram mention of an entity. 
 * The original text is a substring in the input of the given offset and length.
 * Computes keyphraseness, document frequency, candidate entities with compatibility scores and a
 * unique negative id.
 */
public class Mention implements Comparable<Mention> {
	public static int WINDOW_SIZE = 50;
	
	private String ngram;
	private String originalNgram; // Unnormalized.
	private int offset;
	private int length;
	private double keyphraseness;
	private HashMap<Integer, BigDecimal> candidates;
	private double importance;
	private int df;
	// Optional.
	private String filename = "";
	
	public Mention(String ngram, int offset, int length) {
		this.ngram = ngram;
		this.offset = offset;
		this.length = length;
		this.candidates = new HashMap<Integer, BigDecimal>();
	}
	
	public Mention(Ngram ngram) {
		this.ngram = ngram.getNgram();
		this.originalNgram = ngram.getOriginalNgram();
		this.offset = ngram.getOffset();
		this.length = ngram.getLength();
		this.candidates = new HashMap<Integer, BigDecimal>();
	}
	
	public String getNgram() {
		return ngram;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public int getLength() {
		return length;
	}
	
	public String getOriginalNgram() {
		return originalNgram;
	}
	
	public void setOriginalNgram(String originalNgram) {
		this.originalNgram = originalNgram;
	}
	
	public double getKeyphraseness() {
		return keyphraseness;
	}
	
	public Set<Integer> getCandidateEntities() {
		return candidates.keySet();
	}
	
	public BigDecimal getEntityCompatibilityScore(Integer entity) {
		return candidates.get(entity);
	}
	
	public void setEntityCompatibilityScore(Integer entity, BigDecimal score) {
		candidates.put(entity, score);
	}
	
	public int getCandidatesCount() {
		return candidates.size();
	}
	
	public double getImportance() {
		return importance;
	}
	
	public void setImportance(double value) {
		importance = value;
	}
	
	public int getDocumentFrequency() {
		return df;
	}
	
	public void setDocumentFrequency(int value) {
		df = value;
	}
	
	public String getFilename() {
		return filename;
	}
	
	public void setFilename(String filename) {
		this.filename = filename;
	}
	
	public String getContext(String filePath) throws IOException {
		String content = IITBDataset.getFileContent(filePath);
		return content.substring(
				Math.max(0, offset - 50), 
				Math.min(offset + length + 50, content.length())
		);
	}
	
	public void computeKeyphrasenessAndDF(CandidatesIndex mentionEntityIndex) {
		PairOfInts pair = mentionEntityIndex.getKeyphraseness(ngram);
		df = pair.getRightElement();
		keyphraseness = (double) pair.getLeftElement() / df;
	}
	
	public BigDecimal computeSumCompatibilities() {
		BigDecimal result = BigDecimal.ZERO;
		for (BigDecimal score: candidates.values()) {
			result = result.add(score);
		}
		return result;
	}
	
	public Set<Map.Entry<Integer, BigDecimal>> getEntitiesAndScores() {
		return candidates.entrySet();
	}
	
	/**
	 * Extracts the tokens near the mention at most WINDOW_SIZE distance.
	 * @param allContext List of input tokens in ascending order by offset.
	 */
	public List<String> extractContext(List<Token> allContext) {
		int start = 0;
		int end = allContext.size() - 1;
		int middle = (start + end + 1) / 2;
		while (start < end) {
			if (allContext.get(middle).getOffset() == offset) {
				break;
			} else if (allContext.get(middle).getOffset() > offset) {
				end = middle - 1;
			} else {
				start = middle;
			}
			middle = (start + end + 1) / 2;
		}

		int fromIndex, toIndex;
		if (middle - WINDOW_SIZE / 2 < 0) {
			fromIndex = 0;
			toIndex = Math.min(allContext.size(), fromIndex + WINDOW_SIZE);
		} else if (middle + WINDOW_SIZE / 2 >= allContext.size()){
			toIndex = allContext.size();
			fromIndex = Math.max(0, toIndex - WINDOW_SIZE);
		} else {
			fromIndex = middle - WINDOW_SIZE / 2;
			toIndex = fromIndex + WINDOW_SIZE;
		}

		ArrayList<String> result = new ArrayList<String>();
		for (Token token: allContext.subList(fromIndex, toIndex)) {
			result.add(token.getToken());
		}
		return result;
	}
	
	/**
	 * Checks two pairs for equality.
	 *
	 * @param obj object for comparison
	 * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code> otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Mention other = (Mention) obj;
		return ngram.equals(other.getNgram()) && offset == other.getOffset() 
				&& length == other.getLength();
	}

	/**
	 * Defines a natural sort order for Mentions. Mentions are sorted by keyphraseness score.
	 *
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(Mention other) {
		if (other == null) {
			return 1;
		}
		
		if (keyphraseness == other.getKeyphraseness()) {
			if (offset == other.getOffset()) {
				return ngram.compareTo(other.getNgram());					
			}
			return offset < other.getOffset() ? -1 : 1;
		}
		return keyphraseness < other.getKeyphraseness() ? -1 : 1;
	}

	/**
	 * Returns a hash code value for the pair.
	 *
	 * @return hash code for the pair
	 */
	@Override
	public int hashCode() {
		return ngram.hashCode() + offset + length;
	}
	
	@Override
	public String toString() {
		return "ngram:" + ngram + " offset: " + offset + " length:" + length + " original ngram:" + 
				originalNgram + " candidate entities:" + candidates.keySet() + 
				" compatibility scores:" + candidates.values() + 
				" keyphraseness:" + keyphraseness + " document frequency:" + df; 
	}
}
