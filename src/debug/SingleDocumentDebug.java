package debug;

import java.io.IOException;

import iitb.IITBDataset;
import iitb.NameAnnotation;
import index.EntityLinksIndex;
import index.EntityTFIDFIndex;
import index.MentionIndex;
import index.TermDocumentFrequencyIndex;
import index.TitleIDsIndex;
import md.MentionDetection;

import org.apache.hadoop.fs.Path;

import baseline.RandomGraphWalk;


public class SingleDocumentDebug {
	private static final String termDFIndexPath = "/home/marinah/wikipedia/df-index.txt";
	private static final String entityTfIDFFilesPath = "/home/marinah/wikipedia/tf-idf-entity";
	private static final String entityTfIDFIndexPath = 
			"/home/marinah/wikipedia/tf-idf-entity-index.txt";
	private static final String mentionIndexPath = 
			"/home/marinah/wikipedia/mention-entity-keyphraseness.txt";
	private static final String entityLinksIndexPath = 
			"/home/marinah/wikipedia/entity-entity-index.txt";
	private static final String titlesFilePath = "/home/marinah/wikipedia/enwiki-titles.txt";
	
	public static TitleIDsIndex titleIdsIndex;
	
	public static void main(String args[]) throws IOException {
		
		MentionIndex mentionIndex = MentionIndex.load(mentionIndexPath);
		System.out.println("Loaded mention index.");

		EntityTFIDFIndex entityTFIDFIndex = new EntityTFIDFIndex(entityTfIDFFilesPath);
		entityTFIDFIndex.load(new Path(entityTfIDFIndexPath));
		System.out.println("Loaded tf-idf entity index.");

		TermDocumentFrequencyIndex dfIndex = TermDocumentFrequencyIndex.load(termDFIndexPath);
		System.out.println("Loaded term document frequency index.");

		EntityLinksIndex entityLinksIndex = EntityLinksIndex.load(entityLinksIndexPath);
		System.out.println("Loaded entity links index.");

		titleIdsIndex = TitleIDsIndex.load(titlesFilePath); 
		System.out.println("Loaded title ids index.");

	  String content = IITBDataset.getFileContent(
	  		"/home/marinah/input/crawledDocs/ganeshTestDoc.txt");
	  MentionDetection md = new MentionDetection(content, mentionIndex, entityTFIDFIndex, dfIndex);
	  md.setThreshold(0.8, true);
		RandomGraphWalk rgw = new RandomGraphWalk(entityLinksIndex);
		rgw.solve(md.solve());
		
		System.out.println("Solution");
		for (NameAnnotation annotation: rgw.getSolutionNameAnnotations("unused.txt")) {
			System.out.println("Name:" + annotation.getName() + " Entity:" + 
					titleIdsIndex.get(annotation.getEntity()) + " Filename:" + annotation.getFilename());
		}
	}
}
