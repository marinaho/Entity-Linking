package loopybeliefpropagation;

import index.EntityLinksIndex;
import index.TitleIDsIndex;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import md.Mention;

public class ScorerBasic implements Scorer {
	private static final Logger LOG = Logger.getLogger(ScorerBasic.class);
	
	private static double BETA = 1;
			
	private List<Mention> mentions;
	private RelatednessMap relatednessMap;
	private TitleIDsIndex titleIdsIndex;
	
	public ScorerBasic(List<Mention> mentions, EntityLinksIndex index) {
		LOG.setLevel(Level.INFO);
		this.mentions = mentions;
		relatednessMap = new RelatednessMap(mentions, index);
	}

	@Override
	public BigDecimal computeMessageScore(Mention from, Mention to, int entityFrom, 
			int entityTo, MessagesMap oldMessages) {
		int l = mentions.size();
		BigDecimal relatedness = new BigDecimal(
				2 * BETA * relatednessMap.get(entityFrom, entityTo) / (l * (l - 1)));
		BigDecimal compatibility = from
				.getEntityCompatibilityScore(entityFrom)
				.divide(new BigDecimal(l), RoundingMode.HALF_UP);
		BigDecimal messagesNeighbors = 
				oldMessages.sumNeighborMessages(from, entityFrom, to, mentions);
		return compatibility.add(relatedness).add(messagesNeighbors);
	}
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages) {
		HashMap<Candidate, BigDecimal> result = new HashMap<Candidate, BigDecimal>();
		for (Mention mention: mentions) {
			for (Map.Entry<Integer, BigDecimal> entry: mention.getEntitiesAndScores()) {
				int entity = entry.getKey();
				BigDecimal localCompatibility = entry.getValue();
				BigDecimal neighborMessages = messages.sumNeighborMessages(mention, entity, null, mentions);
				BigDecimal score = computeFinalScore(neighborMessages, localCompatibility);
				if (LOG.isTraceEnabled() && titleIdsIndex != null) {
					LOG.trace("Mention:" + mention.getNgram() + " Entity:" + titleIdsIndex.get(entity) + 
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
			for (Map.Entry<Integer, BigDecimal> entry: mention.getEntitiesAndScores()) {
				int entity = entry.getKey();
				BigDecimal localCompatibility = entry.getValue();
				BigDecimal neighborMessages = messages.sumNeighborMessages(mention, entity, null, mentions);
				BigDecimal score = computeFinalScore(neighborMessages, localCompatibility);
				if (bestEntity == -1 || score.compareTo(bestScore) > 0) {
					bestScore = score;
					bestEntity = entity;
				}
				if (LOG.isDebugEnabled() && titleIdsIndex != null) {
					LOG.debug("Mention:" + mention.getNgram() + " Entity:" + titleIdsIndex.get(bestEntity) + 
							" Score:" + score);
				}
			}
			solution.put(mention, bestEntity);
		}
		return solution;
	}
	
	public BigDecimal computeFinalScore(BigDecimal messagesNeighbors, BigDecimal localCompatibility) {
		int l = mentions.size();
		return messagesNeighbors.add(
				localCompatibility.divide(new BigDecimal(l), RoundingMode.HALF_UP));
	}

	public void setTitlesIdIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
	
	public void setBeta(double beta) {
		BETA = beta;
	}
}
