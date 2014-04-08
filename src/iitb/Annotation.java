package iitb;

/*
 *  Wikipedia entity annotation for a token span.
 *  The token span can be found in the specified file with the given offset and length.
 */
public class Annotation {
	private int entity;
	private String filename;
	private int offset;
	private int length;
	
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
	
	@Override
	public String toString() {
		return "entity:" + entity + " offset:" + offset + " length:" + length + 
				" filename:" + filename;
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
		return new Integer(offset).hashCode() + new Integer(length).hashCode() + filename.hashCode() +
				new Integer(entity).hashCode();
	}
}
