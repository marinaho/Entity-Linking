package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;

import edu.umd.cloud9.io.pair.PairOfInts;

public class EntityLinksEntry implements Writable {
	public static final String INTRA_ENTRY_SEPARATOR = ",";
	public static final String BETWEEN_ENTRY_SEPARATOR = "\t";
	private int totalFrequency;
	// Number of links inside entity page. Not equal to the size of the external links vector!
	private int noLinks;
	private List<PairOfInts> links;
	
	public EntityLinksEntry() {
		links = new ArrayList<PairOfInts>();
	}
	
	/**
	 * Recreates the object from it's string representation obtained by toString() method.
	 */
	public EntityLinksEntry(String input) {
		links = new ArrayList<PairOfInts>();
		String[] tokens = StringUtils.split(input, BETWEEN_ENTRY_SEPARATOR);
		totalFrequency = Integer.parseInt(tokens[0]);
		noLinks = Integer.parseInt(tokens[1]);
		for (String pair: Arrays.asList(tokens).subList(2, tokens.length)) {
			String[] parts = StringUtils.split(pair, INTRA_ENTRY_SEPARATOR);
			int entity = Integer.parseInt(parts[0]);
			int frequency = Integer.parseInt(parts[1]);
			links.add(new PairOfInts(entity, frequency));
		}
	}
	
	public int getTotalFrequency() {
		return totalFrequency;
	}
	
	public void setTotalFrequency(int totalFrequency) {
		this.totalFrequency = totalFrequency;
	}

	public int getNoLinks() {
		return noLinks;
	}
	
	public void setNoLinks(int noLinks) {
		this.noLinks = noLinks;
	}
	
	public void addEntry(int entity, int frequency) {
		links.add(new PairOfInts(entity, frequency));
	}
	
	public List<PairOfInts> getLinks() {
		return links;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		totalFrequency = in.readInt();
		noLinks = in.readInt();
		int numEntries = in.readInt();
		while (numEntries-- != 0) {
			int entity = in.readInt();
			int frequency = in.readInt();
			links.add(new PairOfInts(entity, frequency));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(totalFrequency);
		out.writeInt(noLinks);
		out.writeInt(links.size());
		for (PairOfInts link: links) {
			out.writeInt(link.getKey());
			out.writeInt(link.getValue());
		}
	}
	
	@Override
	public String toString() {
		String prefix = Joiner.on(BETWEEN_ENTRY_SEPARATOR).join(totalFrequency, noLinks);
		StringBuilder result = new StringBuilder(prefix);
		for (PairOfInts link: links) {
			String entry = Joiner.on(INTRA_ENTRY_SEPARATOR).join(link.getKey(), link.getValue());
			result.append(BETWEEN_ENTRY_SEPARATOR + entry);
		}
		return result.toString();
	}
}
