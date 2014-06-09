package index;

public interface LinksIndex {
	public int getPopularity(int entity);
	public int getCocitation(int entity1, int entity2);
}
