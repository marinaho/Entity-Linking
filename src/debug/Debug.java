package debug;

import java.io.IOException;
import java.util.HashSet;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import iitb.Annotation;
import iitb.IITBDataset;
import index.MentionEntitiesFrequencyIndex;
import index.TitleIDsIndex;
import index.TitlesIndex;

public class Debug {
	private static String annotationsFilePath = "/home/marinah/input/CSAW_Annotations (original).xml";
	private static String testFilesFolder = "/home/marinah/input/crawledDocs/";
	private static String titlesIndexPath = "/home/marinah/wikipedia/enwiki-titles.txt";
	private static String redirectIndexPath = "/home/marinah/wikipedia/enwiki-redirect-normalized.txt";
	
	public static void main(String[] args) 
			throws ParserConfigurationException, SAXException, IOException {
		displayDuplicateAnnotations();
		//displayNonCanonicalAnnotations();
	}

	/**
	 * Displays annotations in the same file, offset and length with possibly different entities.
	 * Will display non-canonical annotations that are duplicated.
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static void displayDuplicateAnnotations() 
			throws ParserConfigurationException, SAXException, IOException {
		IITBDataset dataset = 
				new IITBDataset(
						titlesIndexPath,
						"/home/marinah/wikipedia/enwiki-redirect-normalized.txt"
			  );
		dataset.load(annotationsFilePath, testFilesFolder, false);
		System.out.println("Loaded annotations.");
		
		TitleIDsIndex titleIdsIndex = TitleIDsIndex.load("/home/marinah/wikipedia/enwiki-titles.txt"); 
		System.out.println("Loaded title ids index.");
		
		HashSet<Spot> spots = new HashSet<Spot>();
		for (Annotation annotation: dataset.getAnnotations()) {
			Spot spot = new Spot(annotation);
			if (spots.contains(spot)) {
				String content = IITBDataset.getFileContent(testFilesFolder + annotation.getFilename());
				String tokenSpan = content.substring(
						annotation.getOffset(), 
						annotation.getOffset() + annotation.getLength()
				);
				String context = content.substring(
						Math.max(0, annotation.getOffset() - 20), 
						Math.min(annotation.getOffset() + annotation.getLength() + 20, content.length())
				);
				System.out.println("Duplicate in file:" + annotation.getFilename() + 
						" at offset:" + annotation.getOffset() + " token span: " + tokenSpan + " context:" +
						context);
				System.out.println("Candidate entities: " + titleIdsIndex.get(annotation.getEntity()) + 
						" and " + titleIdsIndex.get(spot.getEntity()));
			} else {
				spots.add(spot);
			}
		}
	}
	
	private static void displayNonCanonicalAnnotations() 
			throws ParserConfigurationException, SAXException, IOException {
		IITBDataset dataset = 
				new IITBDataset(
						titlesIndexPath,
						redirectIndexPath
			  );
		dataset.load(annotationsFilePath, testFilesFolder, false);
		System.out.println("Loaded annotations.");
		
		for (Annotation annotation: dataset.getAnnotations()) {
			if (annotation.getEntity() == TitlesIndex.NOT_CANONICAL_TITLE) {
				String content = IITBDataset.getFileContent(testFilesFolder + annotation.getFilename());
				String tokenSpan = content.substring(
						annotation.getOffset(), 
						annotation.getOffset() + annotation.getLength()
				);
				String context = content.substring(
						Math.max(0, annotation.getOffset() - 20), 
						Math.min(annotation.getOffset() + annotation.getLength() + 20, content.length())
				);
				System.out.println("Non canonical annotation:" + annotation.getFilename() + 
						" at offset:" + annotation.getOffset() + " token span: " + tokenSpan + " context:" +
						context);
			}
		}
	}
}
