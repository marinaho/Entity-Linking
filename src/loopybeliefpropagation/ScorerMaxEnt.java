package loopybeliefpropagation;

import index.EntityLinksIndex;
import index.TitleIDsIndex;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import knowledgebase.WikiUtils;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import md.Mention;

public class ScorerMaxEnt implements Scorer {
	private static final Logger LOG = Logger.getLogger(ScorerMaxEnt.class);
	public static final double ALPHA = 0.0000001;
	public static final double CONST = - Math.log(WikiUtils.WIKIPEDIA_ARTICLES_SIZE);
			
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
		double rho = Math.log(index.getPopularity(entityFrom));
		double lambda = computeLambda(entityFrom, entityTo);
		BigDecimal messagesNeighbors = 
				oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
		return messagesNeighbors.add(new BigDecimal(rho + lambda));
	}
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages) {
		HashMap<Candidate, BigDecimal> result = new HashMap<Candidate, BigDecimal>();
		for (Mention mention: mentions) {
			for (Integer entity: mention.getCandidateEntities()) {
				BigDecimal score = computeFinalScore(entity, mention, messages);
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
				BigDecimal score = computeFinalScore(entity, mention, messages);
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
	
	public BigDecimal computeFinalScore(int entity, Mention mention, MessagesMap messages) {
		int popularity = index.getPopularity(entity);
		BigDecimal rho = new BigDecimal(Math.log(popularity));
		BigDecimal msgNeighbors = messages.sumNeighborMessages(mention, entity, null, mentions);
		return rho.add(msgNeighbors);
	}
	
	public double computeLambda(int entity1, int entity2) {
		int popularity1 = index.getPopularity(entity1);
		int popularity2 = index.getPopularity(entity2);
		int cocitation = cocitationMap.get(entity1, entity2);
		if (cocitation <= popularity1 * popularity2 / WikiUtils.WIKIPEDIA_ARTICLES_SIZE) {
			return CONST;
		}
		return Math.log(cocitation) - Math.log(popularity1) - Math.log(popularity2);
	}
	
	public void setTitlesIdIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
}
