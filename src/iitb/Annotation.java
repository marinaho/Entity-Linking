package iitb;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import md.Mention;

/**
 *  Wikipedia entity annotation for a token span.
 *  The token span can be found in the specified file by using the given offset and length.
 */
public class Annotation {
	private int entity;
	private String filename;
	private int offset;
	private int length;
	
	private String tokenSpan;
	private String context;
	
	public Annotation(int entity, int offset, int length, String filename) {
		this.entity = entity;
		this.offset = offset;
		this.length = length;
		this.filename = filename;
	}

	public int getEntity() {
		return entity;
	}

	public int getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}
	
	public String getFilename() {
		return filename;
	}
	
	public String getTokenSpan(String filePath) throws IOException {
		if (tokenSpan == null) {
			computeTokenSpanAndContext(filePath);
		}
		return tokenSpan;
	}
	
	public String getContext(String filePath) throws IOException {
		if (context == null) {
			computeTokenSpanAndContext(filePath);
		}
		return context;
	}
	
	public void computeTokenSpanAndContext(String filePath) throws IOException {
		String content = IITBDataset.getFileContent(filePath);
		tokenSpan = content.substring(offset, offset + length);
		context = content.substring(
				Math.max(0, offset - 50), 
				Math.min(offset + length + 50, content.length())
		);
	}
	
	public NameAnnotation toNameAnnotation(String filePath) throws IOException {
		String tokenSpan = getTokenSpan(filePath);
		return new NameAnnotation(tokenSpan, entity, filename);
	}
	
	@Override
	public String toString() {
		return "entity:" + entity + " offset:" + offset + " length:" + length + " filename:" + filename;
	}
	
	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		
		if (object == null || object.getClass() != this.getClass()) {
			return false;
		}
		
		Annotation other = (Annotation) object;
		return offset == other.offset && length == other.length && filename.equals(other.filename) && 
				entity == other.entity;
	}
	
	@Override
	public int hashCode() {
		return entity + offset + length + filename.hashCode();
	}
	
	public static Set<Annotation> getSolution(HashMap<Mention, Integer> solution, String filename) {
		Set<Annotation> annotations = new HashSet<Annotation>();
		for (Map.Entry<Mention, Integer> entry: solution.entrySet()) {
			Mention mention = entry.getKey();
			int entity = entry.getValue();
			annotations.add(new Annotation(entity, mention.getOffset(), mention.getLength(), filename));
		}
		return annotations;
	}
}
