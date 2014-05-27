package loopybeliefpropagation;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import md.Mention;

public class MessagesMap extends HashMap<Message, BigDecimal> {
	private static final long serialVersionUID = -7591260596853849127L;
	private HashMap<Candidate, BigDecimal> neighborMessagesSum;
	
	public MessagesMap() {
		super();
		neighborMessagesSum = new HashMap<Candidate, BigDecimal> ();
	}
	
	/**
	 * Sums all the messages coming from the mentions in the list going to the source and entity, 
	 * excluding one entity
	 * @param source	All messages, except one, going to this source and entity are summed up.
	 * @param entity All messages, except one, going to this source and entity are summed up.
	 * @param exclude	The message coming from this mention is excluded
	 * @param mentions All neighbour of the source mention (including itself).
	 * 
	 * @return	The computed sum
	 */
	public BigDecimal sumNeighborMessages(Mention source, Integer entity, Mention exclude,
			List<Mention> mentions) {
		Candidate candidate = new Candidate(source, entity);
		if (!neighborMessagesSum.containsKey(candidate)) {
			BigDecimal sum = BigDecimal.ZERO;
			for (Mention from: mentions) {
				if (from.equals(source)) {
					continue;
				}
				sum = sum.add(get(new Message(from, source, entity)));
			}
			neighborMessagesSum.put(candidate, sum);
		}
		BigDecimal result = neighborMessagesSum.get(candidate);
		if (exclude != null) {
			result = result.subtract(get(new Message(exclude, source, entity)));
		}
		return result;
	}
}
