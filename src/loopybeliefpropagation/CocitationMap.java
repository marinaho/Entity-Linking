package loopybeliefpropagation;

import index.EntityLinksIndex;
import index.TitleIDsIndex;

import java.util.HashMap;
import java.util.List;

import md.Mention;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;

public class CocitationMap extends HashMap<PairOfInts, Integer>{
	private static final long serialVersionUID = -8757131927926629911L;

	private static final Logger LOG = Logger.getLogger(CocitationMap.class);

	private TitleIDsIndex titleIdsIndex;
	public CocitationMap(List<Mention> mentions, EntityLinksIndex index) {
		super();
		LOG.setLevel(Level.INFO);
		for (int i = 0; i < mentions.size(); ++i) {
			Mention mention1 = mentions.get(i);
			for (int j = i + 1; j < mentions.size(); ++j) {
				Mention mention2 = mentions.get(j);
				for (int candidate1: mention1.getCandidateEntities()) {
					for (int candidate2: mention2.getCandidateEntities()) {
						int cocitation = index.getCocitation(candidate1, candidate2);
						PairOfInts pair = new PairOfInts(
								Math.min(candidate1, candidate2), 
								Math.max(candidate1, candidate2)
						);
						put(pair, cocitation);
						if (LOG.isTraceEnabled() && titleIdsIndex != null) {
								LOG.trace(titleIdsIndex.get(candidate1) + " " + titleIdsIndex.get(candidate2) + 
										" cocitation:" + cocitation);
						}
					}
				}
			}
		}
	}
	
	public int get(int entity1, int entity2) {
		PairOfInts pair = new PairOfInts(Math.min(entity1, entity2), Math.max(entity1, entity2));
		if (!super.containsKey(pair)) {
			return 0;
		}
		return super.get(pair);
	}
	
	public void setTitleIdsIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
}