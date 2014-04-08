package baseline;

/*
 * Edge of the Referent Graph for Entity Linking.
 * Is set between a mention and a candidate entity, or between two entities (candidates for 
 * different mentions).
 */
public class Edge {
	double weight;
	
	public Edge(double inputWeight) {
		weight = inputWeight;
	}
	
	public double getWeight() {
		return weight;
	}
}
