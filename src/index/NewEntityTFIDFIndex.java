package index;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import data.TFIDFEntry;

/**
 * Stores a mapping of (entity, file and offset).
 * Can be used to load the entity tf-idf term vector by reading from disk the specified file at the
 * given offset.
 * The file loaded should be produced by @see knowledgebase.EntityTFIDFIndexBuilder .
 */
public class NewEntityTFIDFIndex {
  private static final Logger LOG = Logger.getLogger(NewEntityTFIDFIndex.class);
  
  private Configuration conf;

  private int blocks;
  private int[] docnos;
  private int[] offsets;
  private short[] fileno;
  private String collectionPath;

  public NewEntityTFIDFIndex() {
    conf = new Configuration();
  }
  
  public NewEntityTFIDFIndex(String collectionPath) {
    conf = new Configuration();
    this.collectionPath = collectionPath;
  }

  public NewEntityTFIDFIndex(Configuration conf, String collectionPath) {
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

    LOG.trace(blocks + " blocks expected");
    docnos = new int[blocks];
    offsets = new int[blocks];
    fileno = new short[blocks];

    for (int i = 0; i < blocks; i++) {
      docnos[i] = in.readInt();
      offsets[i] = in.readInt();
      fileno[i] = in.readShort();

      if (i > 0 && i % 1000000 == 0)
        LOG.trace(i + " blocks read");
    }

    in.close();
  }

  public String getCollectionPath() {
    return collectionPath;
  }

  public TFIDFEntry getEntityTFIDFVector(int docno) {
    long start = System.currentTimeMillis();

    // trap invalid docnos
    if (docno < getFirstDocno() || docno > getLastDocno())
      return null;

    int idx = Arrays.binarySearch(docnos, docno);

    if (idx < 0) {
      return null;
    }

    try {
      DecimalFormat df = new DecimalFormat("00000");
      Path file = new Path(collectionPath + "/part-" + df.format(fileno[idx]));

      LOG.trace("fetching docno " + docno + ": seeking to " + offsets[idx] + " at " + file);

      SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

      IntWritable key = new IntWritable();
      TFIDFEntry value = new TFIDFEntry();

      reader.seek(offsets[idx]);

      while (reader.next(key)) {
        if (key.get() == docno)
          break;
      }
      reader.getCurrentValue(value);
      reader.close();
      long duration = System.currentTimeMillis() - start;

      LOG.trace(" docno " + docno + " fetched in " + duration + "ms");
      
      return value;
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
