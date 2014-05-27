package loopybeliefpropagation;

import index.EntityLinksIndex;
import index.TitleIDsIndex;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import md.Mention;

public class ScorerMaxEnt implements Scorer {
	private static final Logger LOG = Logger.getLogger(ScorerMaxEnt.class);
	public static final double ALPHA = 0.0000001;
			
	private List<Mention> mentions;
	private	EntityLinksIndex index;
	private CocitationMap cocitationMap;
	private TitleIDsIndex titleIdsIndex;
	
	public ScorerMaxEnt(List<Mention> mentions, EntityLinksIndex index) {
		LOG.setLevel(Level.FATAL);
		this.mentions = mentions;
		this.index = index;
		this.cocitationMap = new CocitationMap(mentions, index);
	}

	
	public BigDecimal computeMessageScore(Mention from, Mention to, int entityFrom, int entityTo, 
			MessagesMap oldMessages) {
		int popularity = index.getPopularity(entityTo);
		double cocitation = ALPHA + cocitationMap.get(entityFrom, entityTo);
		LOG.info("Popularity:" + popularity + " Cocitation:" + cocitation);
		double score = Math.log(cocitation) - Math.log(popularity);
		BigDecimal messagesNeighbors = 
				oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
		return messagesNeighbors.add(new BigDecimal(score));
	}
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages) {
		HashMap<Candidate, BigDecimal> result = new HashMap<Candidate, BigDecimal>();
		for (Mention mention: mentions) {
			for (Integer entity: mention.getCandidateEntities()) {
				int popularity = index.getPopularity(entity);
				BigDecimal rho = new BigDecimal(Math.log(popularity));
				BigDecimal score = messages.sumNeighborMessages(mention, entity, null, mentions);
				score = score.add(rho);
				if (LOG.isInfoEnabled() && titleIdsIndex != null) {
					LOG.info("Mention:" + mention.getNgram() + " Entity:" + titleIdsIndex.get(entity) + 
							" Score:" + score);
				}
				result.put(new Candidate(mention, entity), score);
			}
		}
		return result;
	}
	
	public HashMap<Mention, Integer> computeSolution(MessagesMap messages) {
		HashMap<Mention, Integer> solution = new HashMap<Mention, Integer>();
		for (Mention mention: mentions) {
			BigDecimal bestScore = BigDecimal.ZERO;
			Integer bestEntity = -1;
			for (Integer entity: mention.getCandidateEntities()) {
				int popularity = index.getPopularity(entity);
				BigDecimal rho = new BigDecimal(Math.log(popularity));
				BigDecimal score = messages.sumNeighborMessages(mention, entity, null, mentions);
				score = score.add(rho);
				if (bestEntity == -1 || score.compareTo(bestScore) > 0) {
					bestScore = score;
					bestEntity = entity;
				}
				if (LOG.isInfoEnabled() && titleIdsIndex != null) {
					LOG.info("Solution Mention:" + mention.getNgram() + " Entity:" + 
							titleIdsIndex.get(bestEntity) + " Score:" + score);
				}
			}
			solution.put(mention, bestEntity);
		}
		return solution;
	}
	
	public void setTitlesIdIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
}
