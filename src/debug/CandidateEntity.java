package debug;

import java.math.BigDecimal;

public class CandidateEntity implements Comparable<CandidateEntity>{
	public BigDecimal compatibilityScore;
	public double rankerScore;
	public BigDecimal finalScore;
	public int entityID;
	
	public CandidateEntity(int entityID, BigDecimal compatibilityScore, double rankerScore, 
			BigDecimal finalScore) {
		this.entityID = entityID;
		this.compatibilityScore = compatibilityScore;
		this.finalScore = finalScore;
		this.rankerScore = rankerScore;
	}
	
	@Override
	public int compareTo(CandidateEntity other) {
		return finalScore.compareTo(other.finalScore);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		CandidateEntity other = (CandidateEntity) obj;
		return entityID == other.entityID && 
				compatibilityScore.compareTo(other.compatibilityScore) == 0 &&
				finalScore.compareTo(other.finalScore) == 0;
	}
	
	@Override
	public int hashCode() {
		return entityID + finalScore.hashCode() + compatibilityScore.hashCode();
	}
}
