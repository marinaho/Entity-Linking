package loopybeliefpropagation;

import java.math.BigDecimal;
import java.util.HashMap;

import md.Mention;

public interface Scorer {
	public BigDecimal computeMessageScore(Mention from, Mention to, int entityFrom, int entityTo, 
			MessagesMap oldMessages);
	
	public HashMap<Candidate, BigDecimal> computeScores(MessagesMap messages);
	
	public HashMap<Mention, Integer> computeSolution(MessagesMap messages);
}
