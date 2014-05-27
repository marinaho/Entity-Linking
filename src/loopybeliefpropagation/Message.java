package loopybeliefpropagation;

import md.Mention;


public class Message {
	private Mention from;
	private Mention to;
	private int entity;
	
	public Message(Mention from, Mention to, int entity) {
		this.from = from;
		this.to = to;
		this.entity = entity;
	}
	
	@Override
	public int hashCode() {
		return from.hashCode() + to.hashCode() + entity;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		
		Message other = (Message) obj;
		return from.equals(other.from) && to.equals(other.to) && entity == other.entity;
	}
}
