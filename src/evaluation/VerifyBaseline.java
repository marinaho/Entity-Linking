package evaluation;

import iitb.Annotation;
import iitb.IITBDataset;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;

import baseline.RandomGraphWalk;

public class VerifyBaseline {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations.xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	
	public static void main(String args[]) 
			throws ParserConfigurationException, SAXException, IOException {
		PrintWriter out = new PrintWriter("baselineOutput");
		
		out.println("start");
		IITBDataset iitb = new IITBDataset(
				"/home/marinah/wikipedia/enwiki-titles.txt",
				"/home/marinah/wikipedia/enwiki-redirect-normalized.txt");
		iitb.load(annotationsFilePath);
		System.out.println("Loaded IITB dataset.");
		
		MentionIndex mentionIndex = MentionIndex.load(
				"/home/marinah/wikipedia/mention-entity-keyphraseness.txt");
		System.out.println("Loaded mention index.");
		
		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(
				new Configuration(), "/home/marinah/wikipedia/tf-idf-entity");
		entityTFIDFIndex.load(new Path("/home/marinah/wikipedia/tf-idf-entity-index.txt"));
		System.out.println("Loaded tf-idf entity index.");
		
		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(
				"/home/marinah/wikipedia/df-index.txt");
		System.out.println("Loaded term document frequency index.");

		EntityLinksIndex entityLinksIndex = EntityLinksIndex.load(
				"/home/marinah/wikipedia/entity-entity-index.txt");
		System.out.println("Loaded entity links index.");

		for (double percentMentions = 0.1; percentMentions < 1; percentMentions += 0.1) {
			System.out.println("Percent mentions:" + percentMentions);
			int total = 0;
			Set<Annotation> solution = new HashSet<Annotation>();
			for (String filename: iitb.getFilenames()) {
				String filePath = FilenameUtils.normalize(testFilesFolder + filename);
				String content = IITBDataset.getFileContent(filePath);
				out.println("start random walk");
				RandomGraphWalk rgw = new RandomGraphWalk(filename, content, mentionIndex, entityLinksIndex, 
						entityTFIDFIndex, dfIndex);
				solution.addAll(rgw.solve().keySet());
				if (total % 10 == 0)
					System.out.println("Solved for:" + (++total) + " documents.");
				out.println("finished 1 doc");
			}
		
			Verifier verifier = new Verifier();
			verifier.computeResults(solution, iitb.getAnnotations());
		
			System.out.println("Precision: " + verifier.getPrecision());
			System.out.println("Recall: " + verifier.getRecall());
			
			out.println("Solutions for p=" + percentMentions + " " + Joiner.on(" ").join(solution));
		}
		
		out.close();
	}
}
