package evaluation;

import iitb.NameAnnotation;
import index.MentionEntitiesFrequencyIndex;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import loopybeliefpropagation.LoopyBeliefPropagation;
import loopybeliefpropagation.ScorerBasic;
import loopybeliefpropagation.ScorerFull;
import loopybeliefpropagation.ScorerMaxEnt;
import md.Mention;

import org.xml.sax.SAXException;

public class VerifyEntityDisambiguationLoopy extends VerifyEntityDisambiguationAbstract {
	public static int iterations = 75;
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		new VerifyEntityDisambiguationLoopy().run();
	}
	
	public void run() throws ParserConfigurationException, SAXException, IOException {
//		PrintWriter out = new PrintWriter("debug");
		System.out.println("VerifyEntityDisambiguation Loopy Belief Propagation");
		
		configureLogging();
		
		System.out.println("Loading mention index:" + mentionFreqIndexPath);
		MentionEntitiesFrequencyIndex mentionIndex = 
				MentionEntitiesFrequencyIndex.load(mentionFreqIndexPath);
		
		loadIndices();

		int total = 0;
		double averagePrecision = 0;
		double averageRecall = 0;
		for (String filename: iitb.getFilenames()) {
	//	String filename = "ganeshTestDoc.txt"; {
			System.out.println("Solving for document:" + filename + " Number:" + (++total));
			List<Mention> groundTruthMentions = getGroundTruthNameAnnotations(filename, mentionIndex);
			ScorerFull scorer = new ScorerFull(groundTruthMentions, entityLinksIndex, mentionIndex);
//			ScorerMaxEnt scorer = new ScorerMaxEnt(groundTruthMentions, entityLinksIndex);
//			ScorerBasic scorer = new ScorerBasic(groundTruthMentions, entityLinksIndex);
			scorer.setTitlesIdIndex(titleIdsIndex);
			LoopyBeliefPropagation lbp = new LoopyBeliefPropagation(groundTruthMentions, iterations, 
					scorer);
			lbp.setTitlesIdIndex(titleIdsIndex);
			lbp.solve();
			Set<NameAnnotation> solution = lbp.getSolutionNameAnnotations(filename);
			
			Verifier<NameAnnotation> verifier = verifyNameAnnotations(solution);
			for (NameAnnotation annotation: verifier.getWrongAnnotations()) {
				
			}

			averagePrecision += verifier.getPrecision();
			averageRecall += verifier.getRecall();
		}
	
		averagePrecision /= iitb.getNumDocs();
		averageRecall /= iitb.getNumDocs();

		System.out.println("=============== RESULTS =================");
		System.out.println("Average Precision: " + averagePrecision);
		System.out.println("Average Recall: " + averageRecall);
		System.out.println("Maximum achievable recall " + getMaximumAchievableRecall());
	}
}
