/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package knowledgebase;

import index.EntityTFIDFIndex;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.pair.PairOfStringFloat;

public class LookupWikipediaArticle extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("usage: [entity-tf-idf-index-path]");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    Configuration conf = getConf();
    EntityTFIDFIndex f = new EntityTFIDFIndex(conf, null);
    f.load(new Path(args[0]));

    System.out.println("Type \"[id]\" to lookup documents; first id:" + f.getFirstDocno());
    String cmd = null;
    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("lookup > ");
    while ((cmd = stdin.readLine()) != null) {
        try {
        	List<PairOfStringFloat> tfidfVector = f.getEntityTFIDFVector(Integer.parseInt(cmd));
        	if (tfidfVector == null) {
        		System.out.println("0 entries");
        		continue;
        	}
          for (PairOfStringFloat pair : tfidfVector) {
          	System.out.println(pair.getLeftElement() + " " + pair.getRightElement());
          }
        } catch (NumberFormatException e) {
          System.out.println("Invalid docid " + cmd);
        }
      System.out.print("lookup > ");
    }

    return 0;
  }

  private LookupWikipediaArticle() {}

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupWikipediaArticle(), args);
  }
}
