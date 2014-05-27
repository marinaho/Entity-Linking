package debug;

import iitb.Annotation;

/**
 * Spots are annotations compared only by offset, length and filename. The annotated entity does
 * not count for comparison.
 */
public class Spot {
	private int offset;
	private int length;
	private String filename;
	private int entityId;
	
	public Spot(Annotation annotation) {
		this.offset = annotation.getOffset();
		this.length = annotation.getLength();
		this.filename = annotation.getFilename();
		this.entityId = annotation.getEntity();
	}
	
	public int getEntity() {
		return entityId;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		Spot other = (Spot) obj;
		return other.offset == offset && other.length == length && other.filename.equals(filename);
	}
	
	@Override
	public int hashCode() {
		return offset + length + filename.hashCode();
	}
	
	@Override
	public String toString() {
		return "Entity:" + entityId + " Offset:" + offset + " Length:" + length + 
				" Filename:" + filename;
	}
}
