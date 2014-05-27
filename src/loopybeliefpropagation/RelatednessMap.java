package loopybeliefpropagation;

import index.EntityLinksIndex;

import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import md.Mention;
import edu.umd.cloud9.io.pair.PairOfInts;
import evaluation.VerifyLoopyBeliefPropagation;

public class RelatednessMap extends HashMap<PairOfInts, Double>{
	private static final Logger LOG = Logger.getLogger(RelatednessMap.class);
	
	private static final long serialVersionUID = 4700650877205196328L;
	public static final double EPS = 0.000001;
	
	public RelatednessMap(List<Mention> mentions, EntityLinksIndex index) {
		super();
		LOG.setLevel(Level.INFO);
		for (int i = 0; i < mentions.size(); ++i) {
			Mention mention1 = mentions.get(i);
			for (int j = i + 1; j < mentions.size(); ++j) {
				Mention mention2 = mentions.get(j);
				for (int candidate1: mention1.getCandidateEntities()) {
					for (int candidate2: mention2.getCandidateEntities()) {
						if (candidate1 != candidate2) {
							double relatedness = index.getSemanticRelatedness(candidate1, candidate2);
							if (relatedness < EPS) {
								continue;
							}
							PairOfInts pair = new PairOfInts(
									Math.min(candidate1, candidate2), 
									Math.max(candidate1, candidate2)
							);
							put(pair, relatedness);
							if (LOG.isTraceEnabled()) {
									LOG.trace(VerifyLoopyBeliefPropagation.titleIdsIndex.get(candidate1) + " " +
											VerifyLoopyBeliefPropagation.titleIdsIndex.get(candidate2) + " relatedness:" +
											relatedness);
							}
						}
					}
				}
			}
		}
	}
	
	public double get(int entity1, int entity2) {
		if (entity1 == entity2) {
			return 1.0d;
		}
		PairOfInts pair = new PairOfInts(Math.min(entity1, entity2), Math.max(entity1, entity2));
		if (!super.containsKey(pair)) {
			return 0.0;
		}
		return super.get(pair);
	}
}
