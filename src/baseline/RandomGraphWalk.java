package baseline;

import iitb.Annotation;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import knowledgebase.WikiUtils;

import org.apache.commons.collections15.Transformer;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.io.pair.PairOfInts;
import md.Mention;
import md.MentionDetection;

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
	// Random Graph Walk parameter. Set as in Han et al paper: "Collective Entity Linking in Web Text:
	// A Graph-Based Method.
	private static double ALPHA = 0.1;
	
	private static double EPS = 0.00001;
	
	private String filename;
	private EntityLinksIndex entityLinksIndex;
	private MentionDetection mentionDetection;
	
	
	public RandomGraphWalk(String filename, String text, MentionIndex mentionIndex, 
			EntityLinksIndex entityLinksIndex, EntityTFIDFIndex entityTFIDFIndex, 
			TermDocumentFrequencyIndex dfIndex) throws IOException {
		mentionDetection = new MentionDetection(text, mentionIndex, entityTFIDFIndex, dfIndex);
		this.filename = filename;
		this.entityLinksIndex = entityLinksIndex;
	}	
	
	public RandomGraphWalk(String filename, String text, MentionIndex mentionIndex, 
			EntityLinksIndex entityLinksIndex, EntityTFIDFIndex entityTFIDFIndex, 
			TermDocumentFrequencyIndex dfIndex, double percentMentions) throws IOException {		
		mentionDetection = new MentionDetection(text, mentionIndex, entityTFIDFIndex, dfIndex);
		this.filename = filename;
		this.entityLinksIndex = entityLinksIndex;
		mentionDetection.setPercentMentionsToExtract(percentMentions);
	}	
	
	/*
	 * Extract Referent Graph and runs Random Graph Walk. Returns a mapping of entities to scores.
	 */
	public HashMap<Annotation, Double> solve() throws IOException {
		List<Mention> mentions = mentionDetection.solve();
		
		DirectedSparseGraph<Vertex, Edge> graph = constructGraph(mentions);
		
		printInfoRandomGraphWalk(graph);
		
		PageRankWithPriors<Vertex, Edge> ranker = runPageRankWithPriors(graph);

    Map<Vertex, Double> pageRankScores = new HashMap<Vertex, Double>();
    for (Vertex vertex: graph.getVertices()) {
    	if (!vertex.isMention()) {
    		pageRankScores.put(vertex, ranker.getVertexScore(vertex));
    	}
    }
    
    HashMap<Annotation, Double> solution = new HashMap<Annotation, Double>();
    for (Mention mention: mentions) {
    	double bestScore = -1d;
    	int bestEntity = -1;
    	
    	for (PairOfIntFloat entity: mention.getEntitiesAndScores()) {
    		int entityID = entity.getLeftElement();
    		double score = entity.getRightElement() * 
    				pageRankScores.get(new Vertex(mention.getID(), entityID, 0));
    		if (score > bestScore) {
    			bestScore = score;
    			bestEntity = entityID;
    		}
    	}
    	Annotation annotation = new Annotation(
    			bestEntity,
    			mention.getOffset(), 
    			mention.getLength(),
    			filename
    	);
    	solution.put(annotation, bestScore);
    }
    return solution;
	}
	
	public PageRankWithPriors<Vertex, Edge> runPageRankWithPriors(
			DirectedSparseGraph<Vertex, Edge> graph) {
    Transformer<Edge, Double> edgeWeights = new Transformer<Edge, Double>() {
      @Override
      public Double transform(Edge edge) {
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

		HashMap<PairOfInts, Vertex> entitiesMap = new HashMap<PairOfInts, Vertex>();
		int noEntities = 0;
		for (Mention mention: mentions) {
			int mentionId = mention.getID();
			Vertex mentionVertex = new Vertex(mentionId, mention.getImportance());
			noEntities += mention.getCandidateEntities().length;
			
			double sumCompatibilities = 0;
			for (double score: mention.getEntityCompatibilityScores())
				sumCompatibilities += score;
			
			for (PairOfIntFloat entity: mention.getEntitiesAndScores()) {
				int entityID = entity.getLeftElement();
				float compatibility = entity.getRightElement();
				
				PairOfInts mapKey = new PairOfInts(mentionId, entityID);
				Vertex entityVertex;
				if (entitiesMap.containsKey(mapKey)) {
					entityVertex = entitiesMap.get(entityID);
				} else {
					entityVertex = new Vertex(mentionId, entityID, 0);
					entitiesMap.put(mapKey, entityVertex);
				}
				graph.addEdge(new Edge(compatibility / sumCompatibilities), mentionVertex, entityVertex);
			}
		}
		
		System.out.println("Construct graph: add entity - entity relations");
		System.out.println("Mentions: " + mentions.size() + " Entities: " + noEntities);
		Map<PairOfInts, Double> relatednessMap = new HashMap<PairOfInts, Double>(
				noEntities * noEntities / 2);
		
		for (Mention mention1: mentions) {
			System.out.println("Construct relations for mention " + mention1.getNgram() + 
					" candidates count:" + mention1.getCandidateEntities().length);
			for (Integer entity1: mention1.getCandidateEntities()) {
				double sumRelatedness = 0;
				
				for(Mention mention2: mentions) {
					if (mention2 == mention1) {
						continue;
					}
					
					for (Integer entity2: mention2.getCandidateEntities()) {
						PairOfInts pair;
						if (entity1 < entity2) {
							pair = new PairOfInts(entity1, entity2);
						} else {
							pair = new PairOfInts(entity2, entity1);
						}
						
						double relatedness;
						if (relatednessMap.containsKey(pair)) {
							relatedness = relatednessMap.get(pair);
						} else {
							relatedness = getSemanticRelatedness(entity1, entity2);
							relatednessMap.put(pair, relatedness);
						}
						sumRelatedness += relatedness;
					}
				}
				
				for(Mention mention2: mentions) {
					if (mention2 == mention1) {
						continue;
					}
					for (Integer entity2: mention2.getCandidateEntities()) {
						double relatedness = relatednessMap.get(new PairOfInts(entity1, entity2));
						if (relatedness > EPS) {
							Vertex vertexEntity1 = entitiesMap.get(new PairOfInts(mention1.getID(), entity1));
							Vertex vertexEntity2 = entitiesMap.get(new PairOfInts(mention2.getID(), entity2));
							graph.addEdge(new Edge(relatedness / sumRelatedness), vertexEntity1, vertexEntity2);
						}
					}
				}
			}
		}
		
		System.out.println("Finished constructing graph.");
		return graph;
	}
	
	public void printInfoRandomGraphWalk(DirectedSparseGraph<Vertex, Edge> graph) {
		WeakComponentClusterer<Vertex, Edge> clusterer = new WeakComponentClusterer<Vertex, Edge>();
    Set<Set<Vertex>> components = clusterer.transform(graph);
    int numComponents = components.size();
    System.out.println("Number of components: " + numComponents);
    System.out.println("Number of edges: " + graph.getEdgeCount());
    System.out.println("Number of nodes: " + graph.getVertexCount());
    System.out.println("Random jump factor: " + ALPHA);
	}
	
	public int countOccurences(String substring, String text) {
		int result = 0, pos = 0;
		while ((pos = text.indexOf(substring, pos)) != -1) {
			++result;
			pos += text.length();
		}
		return result;
	}
	
	public double getSemanticRelatedness(int entity1, int entity2) {
		Integer[] a = entityLinksIndex.get(entity1);
		Integer[] b = entityLinksIndex.get(entity2);
		int intersectSize = intersectSize(a, b);
		
		if (intersectSize == 0) {
			return 0;
		}
		
		return 1 - 
				(Math.log(Math.max(a.length, b.length)) - Math.log(intersectSize)) /
				(Math.log(WikiUtils.WIKIPEDIA_SIZE) - Math.log(Math.min(a.length, b.length)));
	}
	
	public int intersectSize(Integer[] a, Integer[] b) {
		if (a == null || b == null) {
			return 0;
		}
		
		if (a.length > b.length) {
			return intersectSize(b, a);
		}
		
		Set<Integer> set = new HashSet<Integer>();
		for (int element: a) {
			set.add(element);
		}
		
		int result = 0;
		for (int element: b) {
			if (set.contains(element)) {
				++result;
			}
		}
		return result;
	}
}
