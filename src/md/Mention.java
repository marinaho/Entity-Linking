package md;

import index.MentionIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfInts;

/*
 * Encapsulates an ngram mention of an entity. 
 * The original text is a substring in the input of the given offset and length.
 * Computes keyphraseness, document frequency, candidate entities with compatibility scores and a
 * unique negative id.
 */
public class Mention implements Comparable<Mention> {
	public static final int WINDOW_SIZE = 50;
	
	private static int ID = 0;
	
	private String ngram;
	private int offset;
	private int length;
	private double keyphraseness;
	private Integer[] candidateEntities;
	private Double[] entityCompatibilityScores;
	private double importance;
	private int df;
	private int id;
	
	public Mention(String ngram, int offset, int length) {
		this.ngram = ngram;
		this.offset = offset;
		this.length = length;
		id = --ID;
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
	
	public double getKeyphraseness() {
		return keyphraseness;
	}
	
	public Integer[] getCandidateEntities() {
		return candidateEntities;
	}
	
	public void setCandidateEntities(Integer[] entities) {
		candidateEntities = entities;
	}
	
	public void setCandidateEntities(MentionIndex mentionEntityIndex) {
		setCandidateEntities(mentionEntityIndex.getCandidateEntities(ngram));
	}
	
	public Double[] getEntityCompatibilityScores() {
		return entityCompatibilityScores;
	}
	
	public void setEntityCompatibilityScores(Double[] scores) {
		entityCompatibilityScores = scores;
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
	
	public int getID() {
		return id;
	}
	
	public void computeKeyphrasenessAndDF(MentionIndex mentionEntityIndex) {
		PairOfInts pair = mentionEntityIndex.getKeyphraseness(ngram);
		df = pair.getRightElement();
		keyphraseness = (double) pair.getLeftElement() / df;
	}
	
	public PairOfIntFloat[] getEntitiesAndScores() {
		PairOfIntFloat[] result = new PairOfIntFloat[candidateEntities.length];
		for (int i = 0; i < candidateEntities.length; ++i) {
			result[i] = new PairOfIntFloat(
					candidateEntities[i], 
					entityCompatibilityScores[i].floatValue());
		}
		return result;
	}
	
	/*
	 * Extracts the tokens near the mention at most WINDOW_SIZE distance.
	 * @param allContext List of input tokens in ascending order by offset.
	 */
	public List<String> extractContext(List<Token> allContext) {
		int startPos = -1, endPos = allContext.size() - 1;
		for (int i = 0; i < allContext.size(); ++i) {
			Token token = allContext.get(i);
			if (startPos == -1 && token.getOffset() >= offset - WINDOW_SIZE) {
				startPos = i;
			} else if (token.getOffset() > offset + WINDOW_SIZE) {
				endPos = i - 1;
				break;
			}
		}
		
		List<String> result = new ArrayList<String>();
		for (Token token: allContext.subList(startPos, endPos + 1)) {
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
		return "ngram:" + ngram + " offset: " + offset + " length:" + length + 
				" candidate entities:" + Arrays.toString(candidateEntities) + 
				" compatibility scores:" + Arrays.toString(entityCompatibilityScores) + 
				" keyphraseness:" + keyphraseness + " document frequency:" + df; 
	}
}
