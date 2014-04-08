package md;

/*
 * Encapsulates a single normalized token from the input text.
 * The offset and length in the original text is kept to compare with ground truth annotations.
 */

public class Token {
	private String token;
	private int offset;
	private int length;
	
	public Token(String token, int offset) {
		this.token = token.toLowerCase();
		this.offset = offset;
		this.length = token.length();
	}

	public String getToken() {
		return token;
	}
	
	public int getOffset() {
		return offset;
	}
	
	public int getLength() {
		return length;
	}
	
	@Override
	public String toString() {
		return "token:" + token + " offset:" + offset + " length:" + length;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Token other = (Token) obj;
		return token.equals(other.getToken()) && offset == other.getOffset() && 
				length == other.getLength();
	}
	
	@Override
	public int hashCode() {
		return token.hashCode() + offset + length;
	}
}
