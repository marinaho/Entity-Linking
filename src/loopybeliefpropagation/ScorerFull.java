package loopybeliefpropagation;

import index.EntityLinksIndex;
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
	public static final double ALPHA = 0.0000000001;
	
	private List<Mention> mentions;
	private	EntityLinksIndex index;
	private MentionEntitiesFrequencyIndex mentionIndex;
	private CocitationMap cocitationMap;
	private TitleIDsIndex titleIdsIndex;
	
	public ScorerFull(List<Mention> mentions, EntityLinksIndex index, 
			MentionEntitiesFrequencyIndex mentionIndex) {
		LOG.setLevel(Level.FATAL);
		this.mentions = mentions;
		this.index = index;
		this.mentionIndex = mentionIndex;
		this.cocitationMap = new CocitationMap(mentions, index);
		System.out.println("Alpha:" + ALPHA);
	}

	
	public BigDecimal computeMessageScore(Mention from, Mention to, int entityFrom, int entityTo, 
			MessagesMap oldMessages) {
		double rho1 = - Math.log(index.getPopularity(entityFrom));
		double beta = Math.log(mentionIndex.getCandidateProbability(from.getNgram(), entityFrom));
		// double h = from.getEntityCompatibilityScore(entityFrom).doubleValue();
		// double gamma = Math.log(h / (1 - h));
		BigDecimal gamma = from.getEntityCompatibilityScore(entityFrom);
		double cocitation = ALPHA + cocitationMap.get(entityFrom, entityTo);
		double lambda = Math.log(cocitation)
				- Math.log(index.getPopularity(entityTo)) 
				- Math.log(index.getPopularity(entityFrom)); 
		BigDecimal messagesNeighbors = 
				oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
		BigDecimal result = messagesNeighbors.add(
		//		new BigDecimal(rho1 + beta + lambda + gamma));
				new BigDecimal(rho1 + beta + lambda)).add(gamma);
		if (LOG.isInfoEnabled() && titleIdsIndex != null) {
			LOG.info("Message from:" + titleIdsIndex.get(entityFrom) + " to:" + 
					titleIdsIndex.get(entityTo) + " Popularity 1:" + index.getPopularity(entityFrom) +
					" Popularity 2:" + index.getPopularity(entityTo) + " Beta:" + beta + " Gamma:" + 
					gamma + " cocitation:" + cocitation + " final score:" + result.doubleValue());
		}
		return result;
	}
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages) {
		HashMap<Candidate, BigDecimal> result = new HashMap<Candidate, BigDecimal>();
		for (Mention mention: mentions) {
			for (Integer entity: mention.getCandidateEntities()) {
				double rho = -Math.log(index.getPopularity(entity));
				double beta = Math.log(mentionIndex.getCandidateProbability(mention.getNgram(), entity));
				BigDecimal gamma = mention.getEntityCompatibilityScore(entity);
				// double h = mention.getEntityCompatibilityScore(entity).doubleValue();
				// double gamma = Math.log(h / (1 - h));
				BigDecimal score = messages.sumNeighborMessages(mention, entity, null, mentions);
				score = score.add(gamma).add(new BigDecimal(rho + beta));
				// score = score.add(new BigDecimal(rho + beta + gamma));
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
				double rho = -Math.log(index.getPopularity(entity));
				double beta = Math.log(mentionIndex.getCandidateProbability(mention.getNgram(), entity));
				BigDecimal gamma = mention.getEntityCompatibilityScore(entity);
				// double h = mention.getEntityCompatibilityScore(entity).doubleValue();
				// double gamma = Math.log(h / (1 - h));
				BigDecimal score = messages.sumNeighborMessages(mention, entity, null, mentions);
				score = score.add(gamma).add(new BigDecimal(rho + beta));
				// score = score.add(new BigDecimal(rho + beta + gamma));
				score = score.add(new BigDecimal(beta));
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
