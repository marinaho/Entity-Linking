package evaluation;

import iitb.NameAnnotation;
import index.EntityLinksFrequencyIndex;
import index.EntityLinksIndex;
import index.MentionEntitiesFrequencyIndex;

import java.io.IOException;
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
		System.out.println("VerifyEntityDisambiguation Loopy Belief Propagation BLA");
		
		configureLogging();
		
		System.out.println("Loading mention index:" + mentionFreqIndexPath);
		MentionEntitiesFrequencyIndex mentionIndex = 
				MentionEntitiesFrequencyIndex.load(mentionFreqIndexPath);
		
		System.out.println("Loading entity links index:" + entityLinksIndexPath);
		entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		
		loadIndices();

		Double betas[] = new Double[]{0.000001, 0.0001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0};
		
		double globalBestBeta = 0, globalBestPrecision = 0, globalBestRecall = 0, globalBestFscore = 0;
		int globalBestThreshold = -1;
		System.out.println("Scorer: " + ScorerFull.class.getName());
		// for (double beta: betas) {
		for (double beta = 0.5; beta < 1.6; beta += 0.1) {
			double bestFscore = 0, bestPrecision = 0, bestRecall = 0;
			int bestThreshold = -1;
			// for (int threshold = 0; threshold <= 5; ++threshold) {
				int total = 0;
				double averagePrecision = 0;
				double averageRecall = 0;
				for (String filename: iitb.getFilenames()) {
					System.out.println("Solving for document:" + filename + " Number:" + (++total));
					List<Mention> groundTruthMentions = getGroundTruthNameAnnotations(filename, mentionIndex);
					// ScorerBasic scorer = new ScorerBasic(groundTruthMentions, entityLinksIndex);
					ScorerFull scorer = new ScorerFull(groundTruthMentions, entityLinksIndex, mentionIndex);
					// ScorerMaxEnt scorer = new ScorerMaxEnt(groundTruthMentions, entityLinksIndex);
					// scorer.setThreshold(threshold);
					scorer.setBeta(beta);
					scorer.setTitlesIdIndex(titleIdsIndex);
					LoopyBeliefPropagation lbp = new LoopyBeliefPropagation(groundTruthMentions, iterations, 
							scorer);
					lbp.setTitlesIdIndex(titleIdsIndex);
					lbp.solve();
					Set<NameAnnotation> solution = lbp.getSolutionNameAnnotations(filename);

					Verifier<NameAnnotation> verifier = verifyNameAnnotations(solution);

					averagePrecision += verifier.getPrecision();
					averageRecall += verifier.getRecall();
				}

				averagePrecision /= iitb.getNumDocs();
				averageRecall /= iitb.getNumDocs();

				System.out.println("=============== RESULTS =================");
				// System.out.println("Threshold:" + threshold + " Beta:" + beta);
				// System.out.println("Threshold:" + threshold);
				System.out.println("Beta:" + beta);
				System.out.println("Average Precision: " + averagePrecision);
				System.out.println("Average Recall: " + averageRecall);
				System.out.println("Maximum achievable recall " + getMaximumAchievableRecall());
				
				double fscore = (averagePrecision + averageRecall) / 2;
				if (fscore > bestFscore) {
					bestFscore = fscore;
				//  bestThreshold = threshold;
					bestPrecision = averagePrecision;
					bestRecall = averageRecall;
				}
			// }
			System.out.println("Best beta:" + beta + " Threshold:" + bestThreshold + " Precision: " +
					bestPrecision + " Recall:" + bestRecall);
			if (bestFscore > globalBestFscore) {
				globalBestFscore = bestFscore;
				globalBestBeta = beta;
				globalBestThreshold = bestThreshold;
				globalBestPrecision = bestPrecision;
				globalBestRecall = bestRecall;
			}
		}
		System.out.println("Global best beta:" + globalBestBeta + " Threshold:" + globalBestThreshold + 
				" Precision: " + globalBestPrecision + " Recall:" + globalBestRecall);
	}
}
