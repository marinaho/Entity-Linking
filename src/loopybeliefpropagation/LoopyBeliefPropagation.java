package loopybeliefpropagation;

import iitb.Annotation;
import iitb.NameAnnotation;
import index.TitleIDsIndex;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import md.Mention;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class LoopyBeliefPropagation {
	private static final Logger LOG = Logger.getLogger(LoopyBeliefPropagation.class);
	
	public static final double EPS = 0.00001;
			
	private List<Mention> mentions;
	private Scorer scorer;
	private HashMap<Mention, Integer> solution;
	private TitleIDsIndex titleIdsIndex;
	public int iterations;
	
	public LoopyBeliefPropagation(List<Mention> mentions, int iterations, Scorer scorer) {
		LOG.setLevel(Level.FATAL);
		this.mentions = mentions;
		this.iterations = iterations;
		this.scorer = scorer;
	}
	
	public void solve() {
		LOG.info("Loopy Belief Propagation: maximum iterations: " + iterations + " mentions:" + 
				mentions.size() + " Scorer:" + scorer.getClass().getName());
		long start = System.currentTimeMillis();
		MessagesMap oldMessages = initializeMessages();
		MessagesMap newMessages = null;
		
		HashMap<Candidate, BigDecimal> scores = null, prevScores = null;
		double delta = 0.0;
		int iteration;
		for (iteration = 1; iteration <= iterations; ++iteration) {
			long startIteration = System.currentTimeMillis();
			newMessages = new MessagesMap();
			for (Mention from: mentions) {
				if (iteration == 1 && LOG.isTraceEnabled() && titleIdsIndex != null) {
					for (Integer entityFrom: from.getCandidateEntities()) {
						BigDecimal compatibility = from.getEntityCompatibilityScore(entityFrom);
						LOG.trace("Local compatibility mention:" + from.getNgram() + " entity:" +
										titleIdsIndex.get(entityFrom) + " score " + compatibility.doubleValue());
					}
				}
				for (Mention to: mentions) {
					if (from.equals(to)) {
						continue;
					}
					boolean uninitialized = true;
					BigDecimal normalizingConstant = BigDecimal.ZERO;
					for (Integer entityTo: to.getCandidateEntities()) {
						BigDecimal bestScore = BigDecimal.ZERO;
						int bestEntity = -1;
						for (Integer entityFrom: from.getCandidateEntities()) {
							BigDecimal score = scorer.computeMessageScore(from, to, entityFrom, entityTo, 
									oldMessages);
						  if (bestEntity == -1 || score.compareTo(bestScore) > 0) {
						  	bestEntity = entityFrom;
						  	bestScore = score;
						  }
							if (LOG.isInfoEnabled() && titleIdsIndex != null) {
								LOG.info("Partial result: from entity:" + titleIdsIndex.get(entityFrom) + 
										" to entity:" + titleIdsIndex.get(entityTo) + " score:" + score.doubleValue());
							}
						}
						if (uninitialized || normalizingConstant.compareTo(bestScore) > 0) {
							uninitialized = false;
							normalizingConstant = bestScore;
						}
						newMessages.put(new Message(from, to, entityTo), bestScore);
						if (LOG.isInfoEnabled() && titleIdsIndex != null) {
							LOG.info("Unnormalized message: from entity:" + titleIdsIndex.get(bestEntity) + 
									" to entity:" + titleIdsIndex.get(entityTo) + " score:" + 
									bestScore.doubleValue());
						}
					}
					LOG.info("Normalizing constant:" + normalizingConstant.doubleValue());
					// Normalize messages.
					for (Integer entityTo: to.getCandidateEntities()) {
						Message message = new Message(from, to, entityTo);
						BigDecimal normalizedScore = newMessages.get(message).subtract(normalizingConstant);
						newMessages.put(message, normalizedScore);
						if (LOG.isInfoEnabled() && titleIdsIndex != null) {
							LOG.info("Message from:" + from.getNgram() + " to:" + to.getNgram() + " to entity:" +
									titleIdsIndex.get(entityTo) + " score:" + normalizedScore.doubleValue());
						}
					}
				}
			}
			scores = scorer.computeScores(newMessages);
			if (iteration != 1) {
				delta = computeDelta(scores, prevScores);
				if (delta < EPS) {
					break;
				}
			} 
			oldMessages = newMessages;
			prevScores = scores;

			long endIteration = System.currentTimeMillis();
			LOG.info("Iteration:" + iteration + " Time spent:" + (endIteration - startIteration));
			LOG.info("Delta:" + delta);
		}
		LOG.info("Converged in " + iteration + " iterations.");
		if (delta > EPS) {
			LOG.info("Finishing delta:" + delta);
		}
		solution = scorer.computeSolution(newMessages);
		long end = System.currentTimeMillis();
		LOG.info("Loopy Belief Propagation spent time: " + (end - start));
	}
	
	private double computeDelta(
			HashMap<Candidate, BigDecimal> scores1, 
			HashMap<Candidate, BigDecimal> scores2) {
		BigDecimal maxDelta = BigDecimal.ZERO;
		for (Map.Entry<Candidate, BigDecimal> entry: scores1.entrySet()) {
			Candidate candidate = entry.getKey();
			BigDecimal score1 = entry.getValue();
			BigDecimal score2 = scores2.get(candidate);
			maxDelta = maxDelta.max(score1.subtract(score2).abs());
		}
		return maxDelta.doubleValue();
	}

	private MessagesMap initializeMessages() {
		MessagesMap result = new MessagesMap();
		for (Mention from: mentions) {
			for (Mention to: mentions) {
				if (from.equals(to)) {
					continue;
				}
				for (Integer entity: to.getCandidateEntities()) {
					Message message = new Message(from, to, entity);
					result.put(message, BigDecimal.ZERO);
				}
			}
		}
		return result;
	}	
	
	public Set<Annotation> getSolutionAnnotations(String filename) {
		Set<Annotation> annotations = new HashSet<Annotation>();
		for (Map.Entry<Mention, Integer> entry: solution.entrySet()) {
			Mention mention = entry.getKey();
			int entity = entry.getValue();
			annotations.add(new Annotation(entity, mention.getOffset(), mention.getLength(), filename));
		}
		return annotations;
	}
	
	public Set<NameAnnotation> getSolutionNameAnnotations(String filename)  {
		Set<NameAnnotation> nameAnnotations = new HashSet<NameAnnotation>();
		for (Map.Entry<Mention, Integer> entry: solution.entrySet()) {
			Mention mention = entry.getKey();
			int entity = entry.getValue();
			nameAnnotations.add(new NameAnnotation(mention.getOriginalNgram(), entity, filename));
		}
		return nameAnnotations;
	}
	
	public void setTitlesIdIndex(TitleIDsIndex titleIdsIndex) {
		this.titleIdsIndex = titleIdsIndex;
	}
}
