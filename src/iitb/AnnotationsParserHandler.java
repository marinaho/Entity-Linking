package iitb;

import index.RedirectPagesIndex;
import index.TitlesIndex;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/*
 * Parses the annotations file from the IITB dataset into a map of (docName, annotations list).
 * Annotations with no coresponding entity are skipped.
 */
public class AnnotationsParserHandler extends DefaultHandler {
	
	private static final String DOC_TAG = "docName";
	private static final String ENTITY_TAG = "wikiName";
	private static final String OFFSET_TAG = "offset";
	private static final String LENGTH_TAG = "length";
	private static final String ANNOTATION_TAG = "annotation";
	
	boolean isDocName;
	boolean isEntity;
	boolean isOffset;
	boolean isLength;
	
	private Set<Annotation> annotations;
	private Set<String> filenames;
	
	private String docName;
	private String entityName;
	private int offset;
	private int length;
	
	private TitlesIndex titlesIndex;
	private RedirectPagesIndex redirectIndex;
 
	public AnnotationsParserHandler(String titlesIndexPath, String redirectIndexPath) 
			throws IOException {
		annotations = new HashSet<Annotation>();
		filenames = new HashSet<String>();
		this.titlesIndex = TitlesIndex.load(titlesIndexPath);
		this.redirectIndex = RedirectPagesIndex.load(redirectIndexPath);
	}
	
	public void startElement(String uri, String localName,	String qName, Attributes attributes) 
			throws SAXException {
		isDocName = isEntity = isOffset = isLength = false;
		switch (qName) {
			case DOC_TAG:
				isDocName = true;
				break;
			case ENTITY_TAG:
				isEntity = true;
				break;
			case OFFSET_TAG:
				isOffset = true;
				break;
			case LENGTH_TAG:
				isLength = true;
		}
	}
 
	public void endElement(String uri, String localName, String qName) throws SAXException {
		isDocName = isEntity = isOffset = isLength = false;
		
		// Annotations with no corresponding entity are skipped.
		if (qName.equals(ANNOTATION_TAG) && !entityName.equals("")) {
			int entityId = titlesIndex.getTitleId(redirectIndex.getRedirect(entityName));
			if (entityId == -1) {
				return;
			}
			Annotation annotation = new Annotation(entityId, offset, length, docName);		
			annotations.add(annotation);
			filenames.add(docName);
			docName = entityName = "";
		}
	}
	
	public void characters(char ch[], int start, int len) throws SAXException {
		if (isDocName) {
			docName = new String(ch, start, len);
		} else if (isEntity) {
			entityName = new String(ch, start, len);
		} else if (isOffset) {
			offset = Integer.parseInt(new String(ch, start, len));
		} else if (isLength) {
			length = Integer.parseInt(new String(ch, start, len));
		}
	}

	public Set<Annotation> getAnnotations() {
		return annotations;
	}
	
	public Set<String> getFilenames() {
		return filenames;
	}
}

