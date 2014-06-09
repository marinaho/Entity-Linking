package evaluation;

import iitb.NameAnnotation;
import index.EntityLinksIndex;
import index.MentionEntitiesFrequencyIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;

import org.xml.sax.SAXException;

import baseline.MostFrequentEntity;

public class VerifyEntityDisambiguationFreq extends VerifyEntityDisambiguationAbstract {
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		System.out.println("VerifyEntityDisambiguation Most Frequent Entity");
		new VerifyEntityDisambiguationFreq().run();
	}
	
	public void run() throws ParserConfigurationException, SAXException, IOException {
		configureLogging();
		
		MentionEntitiesFrequencyIndex mentionIndex = 
				MentionEntitiesFrequencyIndex.load(mentionIndexPath);
		System.out.println("Loaded mention index:" + mentionIndexPath);
		
		System.out.println("Loading entity links index:" + entityLinksIndexPath);
		entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		
		loadIndices();
		
		int total = 0;
		double averagePrecision = 0;
		double averageRecall = 0;
		for (String filename: iitb.getFilenames()) {
			System.out.println("Solving for document:" + filename + " Number:" + (++total));
			
			List<Mention> groundTruthMentions = getGroundTruthNameAnnotations(filename, mentionIndex);
			
			MostFrequentEntity entityDisambiguator = new MostFrequentEntity(mentionIndex);
			HashMap<Mention, Integer> solutionMap = entityDisambiguator.solve(groundTruthMentions);
			Set<NameAnnotation> solution = NameAnnotation.getSet(solutionMap, filename);
			
			Verifier<NameAnnotation> verifier = verifyNameAnnotations(solution);
			
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
