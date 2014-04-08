package baseline;

/*
 * Vertex of the Referent Graph for Entity Linking.
 * Represents a mention or an entity.
 * If it represents a mention the prior is set to the mentions importance. Otherwise it is set to 0.
 * Mentions have negative ids. An entity id is it's integer mapping. See @index.TitlesIndex .
 */
public class Vertex {
	private static int IS_MENTION = 2;
	
	private double prior;
	private int entityid;
	private int mentionid;
	
	public Vertex(int mentionid, double prior) {
		this.mentionid = mentionid;
		this.prior = prior;
		this.entityid = IS_MENTION; 
	}
	
	public Vertex(int mentionid, int entityid, double prior) {
		this.mentionid = mentionid;
		this.entityid = entityid;
		this.prior = prior; 
	}
	
	public double getPrior() {
		return prior;
	}
	
	public int getMentionID() {
		return mentionid;
	}
	
	public int getEntityID() {
		return entityid;
	}
	
	public boolean isMention() {
		return entityid == IS_MENTION;
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
		return mentionid == other.mentionid && entityid == other.entityid;
	}
	
	@Override
	public int hashCode() {
		return mentionid + entityid;
	}
}
