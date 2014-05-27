package knowledgebase;

import index.EntityTFIDFIndex;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import data.TFIDFEntry;

/**
 * Looks up a tf-idf vector of a wikipedia article. The Wikipedia article is given by it's document
 * id. This is computed by using Cloud9. See step 2a) from EntityMentionIndexBuilder.
 */
public class LookupWikipediaArticle extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("usage: [entity-tf-idf-index-path] [entity-tf-idf-files-directory]");
      return -1;
    }

    EntityTFIDFIndex f = new EntityTFIDFIndex(getConf(), args[1]);
    f.load(new Path(args[0]));

    System.out.println("Type \"[id]\" to lookup documents; first id:" + f.getFirstDocno());
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    
    while (true) {
    	System.out.print("lookup > ");
    	if ((cmd = stdin.readLine()) == null) {
    		return 0;
    	}
      try {
      	TFIDFEntry tfidfVector = f.getEntityTFIDFVector(Integer.parseInt(cmd));
       	if (tfidfVector == null) {
       		System.out.println("0 entries");
       		continue;
       	}
       	
       	for (Map.Entry<String, Double> entry: tfidfVector.entrySet()) {
       		System.out.println(entry.getKey() + " " + entry.getValue());
       	}
      } catch (NumberFormatException e) {
       System.out.println("Invalid docid " + cmd);
      }
    }
  }

  private LookupWikipediaArticle() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupWikipediaArticle(), args);
  }
}
