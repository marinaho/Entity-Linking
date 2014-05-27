package md;

/*
 * Ngram (multiple tokens) with offset and length in the original text. 
 * Unnormalized ngram is stored in originalNgram .
 * The normalized ngram has most of the punctuation remove (,; ...), except for some (.! ...).
 */
public class Ngram {
	private StringBuilder normalizedNgram;
	private String originalNgram;
	private int offset;
	private int length;
	
	public Ngram(String ngram, int offset, int length) {
		this.normalizedNgram = new StringBuilder(ngram);
		this.offset = offset;
		this.length = length;
	}
	
	public Ngram(Token token) {
		normalizedNgram = new StringBuilder(token.getToken());
		offset = token.getOffset();
		length = token.getLength();
	}
	
	public String getNgram() {
		return normalizedNgram.toString();
	}
	
	public void setNgram(String ngram) {
		normalizedNgram = new StringBuilder(ngram);
	}
	
	public int getOffset() {
		return offset;
	}
	
	public void setOffset(int offset) {
		this.offset = offset;
	}
	
	public int getLength() {
		return length;
	}
	
	public void setLength(int length) {
		this.length = length;
	}

	public String getOriginalNgram() {
		return originalNgram;
	}

	public void setOriginalNgram(String originalNgram) {
		this.originalNgram = originalNgram;
	}
	
	public void append(Token token) {
		normalizedNgram.append(" " + token.getToken());
		length = token.getOffset() + token.getLength() - offset; 
	}
	
	public Ngram copy() {
		return new Ngram(normalizedNgram.toString(), offset, length);
	}
	
	@Override
	public String toString() {
		return "ngram: " + normalizedNgram.toString() + " offset:" + offset + " length:" + length;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Ngram other = (Ngram) obj;
		return getNgram().equals(other.getNgram()) && offset == other.getOffset() && 
				length == other.getLength();
	}
	
	@Override
	public int hashCode() {
		return normalizedNgram.toString().hashCode() + offset + length;
	}
}
