package loopybeliefpropagation;

import md.Mention;

public class Candidate {
	private Mention mention;
	private int entity;
	
	public Candidate(Mention mention, int entity) {
		this.mention = mention;
		this.entity = entity;
	}

	public Mention getMention() {
		return mention;
	}

	public int getEntity() {
		return entity;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Candidate other = (Candidate) obj;
		return mention.equals(other.getMention()) && entity == other.getEntity();
	}
	
	@Override
	public int hashCode() {
		return mention.hashCode() + entity;
	}
}
