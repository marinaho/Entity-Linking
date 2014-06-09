package loopybeliefpropagation;

import index.LinksIndex;
import index.MentionEntitiesFrequencyIndex;
import index.TitleIDsIndex;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import md.Mention;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ScorerFull implements Scorer {
	private static final Logger LOG = Logger.getLogger(ScorerFull.class);
	
	public static final long PAIRS = 9952525348l;
	public static final long LINKS = 133832127;
	public static final double CONST = Math.log(PAIRS) - 2 * Math.log(LINKS);
	
	private static int THRESHOLD = 0;
	private static double BETA = 1;
	
	private List<Mention> mentions;
	private	LinksIndex index;
	private MentionEntitiesFrequencyIndex mentionIndex;
	private CocitationMap cocitationMap;
	private TitleIDsIndex titleIdsIndex;
	
	public ScorerFull(List<Mention> mentions, LinksIndex index, 
			MentionEntitiesFrequencyIndex mentionIndex) {
		LOG.setLevel(Level.FATAL);
		this.mentions = mentions;
		this.index = index;
		this.mentionIndex = mentionIndex;
		this.cocitationMap = new CocitationMap(mentions, index);
	}

	
	public BigDecimal computeMessageScore(Mention from, Mention to, int entityFrom, int entityTo, 
			MessagesMap oldMessages) {
		int l = mentions.size();
		double beta = Math.log(mentionIndex.getCandidateProbability(from.getNgram(), entityFrom)) / l;
		double lambda = computeLambda(entityFrom, entityTo);
		BigDecimal messagesNeighbors = oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
		BigDecimal result = messagesNeighbors.add(new BigDecimal(beta + lambda));
		if (LOG.isInfoEnabled() && titleIdsIndex != null) {
			LOG.info("Message from:" + titleIdsIndex.get(entityFrom) + " to:" + 
					titleIdsIndex.get(entityTo) + " Popularity 1:" + index.getPopularity(entityFrom) +
					"Popularity 2:" + index.getPopularity(entityTo) + " final score:" + result.doubleValue());
		}
		return result;
	}
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages) {
		HashMap<Candidate, BigDecimal> result = new HashMap<Candidate, BigDecimal>();
		for (Mention mention: mentions) {
			for (Integer entity: mention.getCandidateEntities()) {
				BigDecimal score = computeFinalScore(entity, mention, messages);
				if (LOG.isInfoEnabled() && titleIdsIndex != null) {
					LOG.info("Mention:" + mention.getNgram() + " Candidate Entity:" + 
							titleIdsIndex.get(entity) + " Score:" + score);
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
		int l = mentions.size();
		double beta = Math.log(mentionIndex.getCandidateProbability(mention.getNgram(), entity)) / l;
		BigDecimal msgNeighbors = messages.sumNeighborMessages(mention, entity, null, mentions);
		return msgNeighbors.add(new BigDecimal(beta));
	}

	public double computeLambda(int entity1, int entity2) {
		int popularity1 = index.getPopularity(entity1);
		int popularity2 = index.getPopularity(entity2);
		int cocitation = cocitationMap.get(entity1, entity2);
		int l = mentions.size();
		if (cocitation <= THRESHOLD || checkAnticorrelated(cocitation, popularity1, popularity2)) {
			return 2 * BETA * CONST / (l * (l - 1));
		}
		 /* double lambda = 
				Math.log(cocitation) - 
				Math.log(index.getConditionalDenominator(entity1, entity2)) - 
				Math.log(popularity2);
				*/
		double lambda = Math.log(cocitation) - Math.log(popularity1) - Math.log(popularity2);
		return 2 * BETA * lambda / (l * (l - 1));
	}
	
	public boolean checkAnticorrelated(int cocitation, int popularity1, int popularity2) {
		return (double) cocitation / popularity1 / popularity2 <= (double) PAIRS / LINKS / LINKS;
	}
	
	public void setTitlesIdIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
	
	public void setThreshold(int threshold) {
		THRESHOLD = threshold;
	}
	
	public void setBeta(double beta) {
		BETA = beta;
	}
}
