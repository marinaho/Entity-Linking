package iitb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

/*
 * Ingests IITB Dataset: test documents and annotations.
 */
public class IITBDataset {

	private String titlesIndex;
	private String redirectIndex;
	
	private Set<Annotation> annotations;
	private Set<String> filenames;
	
	public IITBDataset(String titlesIndex, String redirectIndex) {
		this.titlesIndex = titlesIndex;
		this.redirectIndex = redirectIndex;
	}
	
	public void load(String annotationsFilePath) 
			throws ParserConfigurationException, SAXException, IOException {
		SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
		AnnotationsParserHandler handler = new AnnotationsParserHandler(titlesIndex, redirectIndex);
		saxParser.parse(annotationsFilePath, handler);	
		annotations = handler.getAnnotations();
		filenames = handler.getFilenames();
	}

	public Set<Annotation> getAnnotations() {
		return annotations;
	}
	
	public Set<String> getFilenames() {
		return filenames;
	}
	
	public Set<Annotation> getAnnotations(String filename) {
		Set<Annotation> result = new HashSet<Annotation>();
		for (Annotation annotation: annotations) {
			if (annotation.getFilename().equals(filename)) {
				result.add(annotation);
			}
		}
		return result;
	}
	
	public static String getFileContent(String filePath) throws IOException {
		String line;
		StringBuilder content = new StringBuilder("");
		
		BufferedReader in = new BufferedReader(new FileReader(filePath));
		while ((line = in.readLine()) != null ) {
			content.append(line + "\n");
		}
		
		in.close();
		return content.toString();
	}
}
