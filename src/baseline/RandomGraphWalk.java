package baseline;

import iitb.Annotation;
import iitb.NameAnnotation;
import index.EntityLinksIndex;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import debug.CandidateEntity;
import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.graph.util.Pair;
import edu.umd.cloud9.io.pair.PairOfInts;
import evaluation.VerifyBaseline;
import md.Mention;

/*
 * Runs Random Graph Walk for entity linking on input text:
 * - Extracts the referent graph of mentions and entities from an input text.
 * - Runs a random graph walk (same as personalized page rank).
 * Needs indices: anchor text index (mapping anchor text to keyphraseness and candidate entities), 
 * entity links index, tf-idf entities index, df term index.
 * @See index.AnchorTextIndex, index.EntityLinksIndex, index.EntityTFIDFIndex, 
 * index.DocumentFrequencyIndex.
 */
public class RandomGraphWalk {
	private static final Logger LOG = Logger.getLogger(RandomGraphWalk.class);
	
	// Random Graph Walk parameter. Set as in Han et al paper: "Collective Entity Linking in Web Text:
	// A Graph-Based Method.
	public static final double ALPHA = 0.1;
	public static final double EPS = 0.00001;
	
	public static double MAP_MAX = 18000;
	
	public static int DEBUG_CANDIDATES = 10;
	
	private EntityLinksIndex entityLinksIndex;
	
	private HashMap<Mention, Integer> solution;
	
	public RandomGraphWalk(EntityLinksIndex entityLinksIndex) throws IOException {
		this.entityLinksIndex = entityLinksIndex;
		LOG.setLevel(Level.INFO);
	}	
	
	/**
	 * Extract Referent Graph and runs Random Graph Walk.
	 */
	public void solve(List<Mention> mentions) 
			throws IOException {
		long start = System.currentTimeMillis();
		DirectedSparseGraph<Vertex, Edge> graph = constructGraph(mentions);
		long end = System.currentTimeMillis();
		LOG.info("Constructing graph spent time: " + (end - start));
	
		if (graph.getVertexCount() == 0) {
			return;
		}
		
		printInfoRandomGraphWalk(graph);
		LOG.info("Verifying graph.");
		verifyGraph(graph);
		
		start = System.currentTimeMillis();
		PageRankWithPriors<Vertex, Edge> ranker = runPageRankWithPriors(graph);
		end = System.currentTimeMillis();
		LOG.info("Page rank spent time: " + (end - start));
  
		computeSolution(ranker, mentions);
		
		verifyRankerScores(graph, ranker);
	}
	
	public void verifyRankerScores(DirectedSparseGraph<Vertex, Edge> graph, 
			PageRankWithPriors<Vertex, Edge> ranker) {
    double sumScores = 0;
    for (Vertex v: graph.getVertices()) {
    	sumScores += ranker.getVertexScore(v);
    }
    if (Math.abs(sumScores - 1) > EPS) {
    	LOG.error("Error! Sum ranker scores not 1:" + sumScores);
    }
	}
	
	public void computeSolution(PageRankWithPriors<Vertex, Edge> ranker, List<Mention> mentions) {
		solution = new HashMap<Mention, Integer>();
    for (Mention mention: mentions) {
    	TreeSet<CandidateEntity> set = new TreeSet<CandidateEntity>(); 
    	int bestEntity = -1;
    	BigDecimal bestScore = new BigDecimal(-1);
    	
    	for (Map.Entry<Integer, BigDecimal> entry: mention.getEntitiesAndScores()) {
    		int entity = entry.getKey();
    		BigDecimal localCompatibility = entry.getValue();
    		double rankerScore = ranker.getVertexScore(new Vertex(entity, mention));
    		if (rankerScore < 0 || rankerScore > 1) {
    			LOG.error("Error! Entity:" + entity + "  ranker score:" + rankerScore);
    		}
    		BigDecimal finalScore = localCompatibility.multiply(new BigDecimal(rankerScore));
    		if (finalScore.compareTo(bestScore) > 0) {
    			bestScore = finalScore;
    			bestEntity = entity;
    		}
    		
    		if (LOG.isTraceEnabled()) {
    			CandidateEntity candidate = new CandidateEntity(entity, localCompatibility, rankerScore, 
    					finalScore);
    			if (set.size() < DEBUG_CANDIDATES) {
    				set.add(candidate);
    			} else {
    				if (set.first().compareTo(candidate) < 0) {
    					set.remove(set.first());
    					set.add(candidate);
    				}
    			}
    		}
    	}
    	solution.put(mention, bestEntity);
    
    	if (LOG.isTraceEnabled() && VerifyBaseline.titleIdsIndex != null) {
    		for  (CandidateEntity candidate: set) {
    			LOG.trace("Mention:" + mention.getOriginalNgram() + " Entity:" + 
    					VerifyBaseline.titleIdsIndex.get(candidate.entityID) + " Ranker score:" +
    					candidate.rankerScore + " Local score" + candidate.compatibilityScore + 
    					" Final score:" + candidate.finalScore);
    		}
    	}
    }
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

	public PageRankWithPriors<Vertex, Edge> runPageRankWithPriors(
			DirectedSparseGraph<Vertex, Edge> graph) {
    Transformer<Edge, BigDecimal> edgeWeights = new Transformer<Edge, BigDecimal>() {
      @Override
      public BigDecimal transform(Edge edge) {
        return edge.getWeight();
      }
    };
    
    Transformer<Vertex, Double> nodePriors = new Transformer<Vertex, Double>() {
      @Override
      public Double transform(Vertex vertex) {
        return vertex.getPrior();
      }
    };
    
    PageRankWithPriors<Vertex, Edge> ranker = new PageRankWithPriors<Vertex, Edge>(graph,
    		edgeWeights, nodePriors, ALPHA);
    
    ranker.evaluate();
    
    return ranker;
	}
	
	public DirectedSparseGraph<Vertex, Edge> constructGraph(List<Mention> mentions) 
			throws IOException {
		DirectedSparseGraph<Vertex, Edge> graph = new DirectedSparseGraph<Vertex, Edge>();
		int noEntities = 0;
		for (Mention mention: mentions) {
			LOG.debug("Mention:" + mention.getOriginalNgram() + " Importance:" + mention.getImportance());
			noEntities += mention.getCandidatesCount();
			
			Vertex mentionVertex = new Vertex(mention, mention.getImportance());
			BigDecimal sumCompatibilities = mention.computeSumCompatibilities();		
			
			// If can happen that all candidates have 0 local compatibility with the mention.
			// We don't want to divide by 0 in this case.
			if (sumCompatibilities.compareTo(BigDecimal.ZERO) == 0) {
				for (Integer entity: mention.getCandidateEntities()) {
					BigDecimal edgeWeight = BigDecimal.ONE.divide(
							new BigDecimal(mention.getCandidatesCount()), RoundingMode.HALF_UP);
					Edge edge = new Edge(edgeWeight);
					Vertex entityVertex = new Vertex(entity, mention);
					graph.addEdge(edge, new Pair<Vertex>(mentionVertex, entityVertex), EdgeType.DIRECTED);
				}
				continue;
			}
			
			for (Map.Entry<Integer, BigDecimal> entry: mention.getEntitiesAndScores()) {
				int entity = entry.getKey();
				BigDecimal compatibility = entry.getValue();
				BigDecimal normalizedCompatibility = compatibility
						.divide(sumCompatibilities, RoundingMode.HALF_UP);			
				Edge edge = new Edge(normalizedCompatibility);
				
				Vertex entityVertex = new Vertex(entity, mention);
				graph.addEdge(edge, new Pair<Vertex>(mentionVertex, entityVertex), EdgeType.DIRECTED);
				
				if (LOG.isDebugEnabled()) {
					LOG.debug("Mention:" + mention.getOriginalNgram() + " Entity:" + entity + 
							" Compatibility:" + compatibility.setScale(4, RoundingMode.HALF_UP) + 
							" Sum compatibilities:" + sumCompatibilities.setScale(4, RoundingMode.HALF_UP));
				}
			}
		}
		LOG.info("EL Graph Mentions: " + mentions.size() + " Entities: " + noEntities);
		
		HashMap<PairOfInts, Double> relatednessMap = null;
		boolean useMap = false;
		if (graph.getVertexCount() < MAP_MAX) {
			relatednessMap = new HashMap<PairOfInts, Double>(1000000);
			useMap = true;

		}		
		for (Mention mention1: mentions) {
			if (mention1.getCandidatesCount() > 2000) {
				System.out.println("Mention " + mention1.getNgram() + " has " + 
						mention1.getCandidatesCount() + " candidates");
			}
			
			for (Integer entity1: mention1.getCandidateEntities()) {
				double sumRelatedness = 0;				
				for (Mention mention2: mentions) {
					if (mention2 == mention1) {
						continue;
					}		
					for (Integer entity2: mention2.getCandidateEntities()) {
						double relatedness = computeRelatedness(entity1, entity2, relatednessMap, useMap);
						if (relatedness > EPS) {
							sumRelatedness += relatedness;
						}
					}
				}
				
				for (Mention mention2: mentions) {
					if (mention2 == mention1) {
						continue;
					}
					for (Integer entity2: mention2.getCandidateEntities()) {
						double relatedness = computeRelatedness(entity1, entity2, relatednessMap, useMap);
						if (relatedness > EPS) {
							LOG.debug(entity1 + " to "  + entity2 + " Relatedness:" + relatedness);
							Vertex vertex1 = new Vertex(entity1, mention1);
							Vertex vertex2 = new Vertex(entity2, mention2);
							Edge edge = new Edge(new BigDecimal(relatedness / sumRelatedness));
							graph.addEdge(edge, new Pair<Vertex>(vertex1, vertex2), EdgeType.DIRECTED);
						}
					}
				}
			}
		}
		
		return graph;
	}
	
	public void verifyGraph(DirectedSparseGraph<Vertex, Edge> graph) {
		BigDecimal eps = new BigDecimal(EPS);
		double sumPriors = 0;
		for (Vertex v: graph.getVertices()) {
			double prior = v.getPrior();
			if (prior < 0 || prior > 1) {
				LOG.error("Error Prior! Vertex " + v.getMention().getNgram() + " " + v.getEntity() + 
						"Prior:" + v.getPrior());
			}
			sumPriors += prior;
			
			BigDecimal sumRelatedness = BigDecimal.ZERO;
			for (Edge e: graph.getIncidentEdges(v)) {
				if (graph.isSource(v, e)) {
					BigDecimal weight = e.getWeight();
					if (weight.compareTo(BigDecimal.ZERO) < 0 || weight.compareTo(BigDecimal.ONE) > 0) {
						LOG.error("Error relatedness! Vertex:" + v.getMention().getNgram() + " " + 
								v.getEntity() + " score:" + weight);
					}
					sumRelatedness = sumRelatedness.add(e.getWeight());
				}
			}
			if (graph.getSuccessorCount(v) != 0 && 
					sumRelatedness.subtract(BigDecimal.ONE).abs().compareTo(eps) > 0) {
				LOG.error("Error sum relatedness! Vertex:" + v.getMention().getNgram() + " " + 
						v.getEntity() + " score:" + sumRelatedness + " outgoing edges:" + 
						graph.getSuccessorCount(v));
			}
		}
		
		if (Math.abs(sumPriors - 1) > EPS) {
			LOG.error("Error! Sum priors=" + sumPriors);
			for (Vertex v: graph.getVertices()) {
				if (v.getPrior() <= 0d) {
					continue;
				}
				System.out.println(v.getMention().getOriginalNgram() + " " + v.getEntity() + " " + v.getPrior());
			}
		}
	}
	
	public double computeRelatedness(int entity1, int entity2, 
			HashMap<PairOfInts, Double> relatednessMap, boolean useMap) {
		if (!useMap) {
			return entityLinksIndex.getSemanticRelatedness(entity1, entity2);
		}
		
		PairOfInts pair = new PairOfInts(
				Math.min(entity1, entity2),
				Math.max(entity1, entity2)
		);
		if (relatednessMap.containsKey(pair)) {
			return relatednessMap.get(pair);
		}
		double relatedness = entityLinksIndex.getSemanticRelatedness(entity1, entity2);
		relatednessMap.put(pair, relatedness);
		return relatedness;
	}
	
	public void printInfoRandomGraphWalk(DirectedSparseGraph<Vertex, Edge> graph) {
		WeakComponentClusterer<Vertex, Edge> clusterer = new WeakComponentClusterer<Vertex, Edge>();
    Set<Set<Vertex>> components = clusterer.transform(graph);
    int numComponents = components.size();
    LOG.info("Number of components: " + numComponents + " edges: " + 
    		graph.getEdgeCount() + " nodes: " + graph.getVertexCount());
	}
	
	public static int countOccurences(String substring, String text) {
		int result = 0, pos = 0;
		while ((pos = text.indexOf(substring, pos)) != -1) {
			++result;
			pos += text.length();
		}
		return result;
	}
}
