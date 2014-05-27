package baseline;

import md.Mention;

/*
 * Vertex of the Referent Graph for Entity Linking.
 * Represents a mention or an entity.
 * If it represents a mention the prior is set to the mentions importance. Otherwise it is set to 0.
 * Mentions have negative ids. An entity id is it's integer mapping. See @index.TitlesIndex .
 */
public class Vertex {
	public static final int NOT_SET = -1;
	private int entity;
	private Mention mention;
	private double prior;
	
	public Vertex(Mention mention, Double prior) {
		this.mention = mention;
		this.prior = prior;
		this.entity = NOT_SET;
	}
	
	public Vertex(Integer entity, Mention mention) {
		this.entity = entity;
		this.mention = mention;
		this.prior = 0; 
	}
	
	public double getPrior() {
		return prior;
	}
	
	public Mention getMention() {
		return mention;
	}
	
	public int getEntity() {
		return entity;
	}
	
	public boolean isMention() {
		return entity == NOT_SET;
	}
	
	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		
		if (object == null || object.getClass() != this.getClass()) {
			return false;
		}
		
		Vertex other = (Vertex) object;
		return mention.equals(other.getMention()) && entity == other.getEntity();
	}
	
	@Override
	public int hashCode() {
		return mention.hashCode() + entity;
	}
}
