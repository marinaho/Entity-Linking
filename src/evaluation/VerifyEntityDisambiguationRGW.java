package evaluation;

import iitb.NameAnnotation;
import index.EntityLinksIndex;
import index.MentionIndex;
import index.TitleIDsIndex;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import md.Mention;

import org.xml.sax.SAXException;

import baseline.RandomGraphWalk;

public class VerifyEntityDisambiguationRGW extends VerifyEntityDisambiguationAbstract {
	public static TitleIDsIndex titleIdsIndex;
		
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		new VerifyEntityDisambiguationRGW().run();
	}
	
	public void run() throws ParserConfigurationException, SAXException, IOException {
		System.out.println("VerifyEntityDisambiguation Random Graph Walk");

		configureLogging();
		
		MentionIndex mentionIndex = MentionIndex.load(mentionIndexPath);
		System.out.println("Loaded mention index:" + mentionIndexPath);
		
		System.out.println("Loading entity links index:" + entityLinksIndexPath);
		EntityLinksIndex entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		
		loadIndices();
		
		RandomGraphWalk rgw = new RandomGraphWalk(entityLinksIndex);
		
		double averagePrecision = 0;
		double averageRecall = 0;
		int total = 0;
		setSkipZeroCompatibilityMentions();
		for (String filename: iitb.getFilenames()) {

			System.out.println("Solving for document:" + filename + " Number:" + (++total));
			List<Mention> groundTruthMentions = getGroundTruthNameAnnotations(filename, mentionIndex);
			rgw.solve(groundTruthMentions);
			Set<NameAnnotation> solution = rgw.getSolutionNameAnnotations(filename);
			
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
