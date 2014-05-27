package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class TFIDFEntry extends HashMap<String, Double> implements Writable {
	private static final long serialVersionUID = -7052278145882074957L;

	public TFIDFEntry() {	
	}
	
	public TFIDFEntry(int size) {
		super(size * 4 / 3 + 1);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int numEntries = in.readInt();
		while (numEntries-- > 0) {
			put(in.readUTF(), in.readDouble());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (Map.Entry<String, Double> entry: entrySet()) {
			out.writeUTF(entry.getKey());
			out.writeDouble(entry.getValue());
		}
	}
}
