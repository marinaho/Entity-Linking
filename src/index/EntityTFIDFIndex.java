package index;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.pair.PairOfStringFloat;

public class EntityTFIDFIndex {
  private static final Logger LOG = Logger.getLogger(EntityTFIDFIndex.class);
  
  private Configuration conf;

  private int blocks;
  private int[] docnos;
  private int[] offsets;
  private short[] fileno;
  private String collectionPath;

  public EntityTFIDFIndex() {
    conf = new Configuration();
  }

  public EntityTFIDFIndex(Configuration conf, String collectionPath) {
    this.conf = Preconditions.checkNotNull(conf);
  	this.collectionPath = collectionPath;
  }

  public void load(Path filename) throws IOException {
  	FileSystem fs = FileSystem.getLocal(conf);
    FSDataInputStream in = fs.open(filename);

    // Class name; throw away.
    in.readUTF();
    // Collection Path; may have moved; throw away.
    in.readUTF();

    blocks = in.readInt();

    LOG.info(blocks + " blocks expected");
    docnos = new int[blocks];
    offsets = new int[blocks];
    fileno = new short[blocks];

    for (int i = 0; i < blocks; i++) {
      docnos[i] = in.readInt();
      offsets[i] = in.readInt();
      fileno[i] = in.readShort();

      if (i > 0 && i % 1000000 == 0)
        LOG.info(i + " blocks read");
    }

    in.close();
  }

  public String getCollectionPath() {
    return collectionPath;
  }

  public List<PairOfStringFloat> getEntityTFIDFVector(int docno) {
    long start = System.currentTimeMillis();

    // trap invalid docnos
    if (docno < getFirstDocno() || docno > getLastDocno())
      return null;

    int idx = Arrays.binarySearch(docnos, docno);

    ArrayList<PairOfStringFloat> result = new ArrayList<PairOfStringFloat>();
    if (idx < 0) {
      return result;
    }

    try {
      DecimalFormat df = new DecimalFormat("00000");
      Path file = new Path(collectionPath + "/part-" + df.format(fileno[idx]));

      LOG.info("fetching docno " + docno + ": seeking to " + offsets[idx] + " at " + file);

      SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(file));

      LongWritable key = new LongWritable();
      Text value = new Text();

      reader.seek(offsets[idx]);

      while (reader.next(key)) {
        if (key.get() == docno)
          break;
      }
      reader.getCurrentValue(value);
      reader.close();
      long duration = System.currentTimeMillis() - start;

      LOG.info(" docno " + docno + " fetched in " + duration + "ms");
      
      String[] parts = value.toString().split("\t");
      for (int i = 0; i < parts.length; i += 2) {
      	result.add(new PairOfStringFloat(parts[i], Float.parseFloat(parts[i + 1])));
      }
      return result;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }


  public int getFirstDocno() {
    return docnos[0];
  }

  public int getLastDocno() {
    return docnos[blocks - 1];
  }
}
