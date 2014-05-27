package baseline;

import java.math.BigDecimal;

/**
 * Edge of the Referent Graph for Entity Linking. Encapsulates the weight of the edge.
 */
public class Edge {
	BigDecimal weight;
	
	public Edge(BigDecimal inputWeight) {
		weight = inputWeight;
	}
	
	public BigDecimal getWeight() {
		return weight;
	}	
}
