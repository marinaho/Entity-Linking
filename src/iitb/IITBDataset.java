package iitb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.io.FilenameUtils;
import org.xml.sax.SAXException;

/**
 * Ingests IITB Dataset: test documents and annotations.
 */
public class IITBDataset {

	private String titlesIndex;
	private String redirectIndex;
	private String testFilesFolder;
	
	private Set<NameAnnotation> nameAnnotations;
	private Set<Annotation> annotations;
	private Set<String> filenames;
	
	public IITBDataset(String titlesIndex, String redirectIndex) {
		this.titlesIndex = titlesIndex;
		this.redirectIndex = redirectIndex;
	}
	
	public void load(String annotationsFilePath, String testFilesFolder, boolean skipNonCanonical) 
			throws ParserConfigurationException, SAXException, IOException {
		this.testFilesFolder = testFilesFolder;
		SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
		AnnotationsParserHandler handler = new AnnotationsParserHandler(
				titlesIndex, redirectIndex, skipNonCanonical);
		saxParser.parse(annotationsFilePath, handler);	
		annotations = handler.getAnnotations();
		filenames = handler.getFilenames();
	}

	public Set<Annotation> getAnnotations() {
		return annotations;
	}
	
	public Set<NameAnnotation> getNameAnnotations() throws IOException {
		if (nameAnnotations == null && annotations != null) {
			nameAnnotations = new HashSet<NameAnnotation>();
			for (Annotation annotation: annotations) {
				String filePath = FilenameUtils.normalize(testFilesFolder + annotation.getFilename());
				nameAnnotations.add(annotation.toNameAnnotation(filePath));
			}
		}
		return nameAnnotations;
	}
	
	public Set<String> getFilenames() {
		return filenames;
	}
	
	
	public int getNumDocs() {
		return filenames.size();
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
	
	public Set<NameAnnotation> getNameAnnotations(String filename) throws IOException {
		if (nameAnnotations == null) {
			getNameAnnotations();
		}
		Set<NameAnnotation> result = new HashSet<NameAnnotation>();
		for (NameAnnotation nameAnnotation: nameAnnotations) {
			if (nameAnnotation.getFilename().equals(filename)) {
				result.add(nameAnnotation);
			}
		}
		return result;
	}
	
	public int getNameAnnotationsCount() {
		return nameAnnotations.size();
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
	
	public static String getTokenSpan(String filePath, int offset, int length) throws IOException {
		String content = getFileContent(filePath);
		return content.substring(offset, offset + length);
	}
}
