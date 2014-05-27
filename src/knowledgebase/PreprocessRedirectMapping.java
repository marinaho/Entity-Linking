package knowledgebase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import com.google.common.base.Joiner;

import normalizer.Normalizer;

/**
 * Processes the redirect pages mapping obtained by https://code.google.com/p/wikipedia-redirect/
 * Fixes encodings and transforms titles to canonical forms.
 */
public class PreprocessRedirectMapping {
	public static void main(String args[]) throws IOException {
		if (args.length < 2) {
			System.out.println("Usage: java PreprocessRedirectMapping <redirect-mapping-filename> "
					+ "<output-filename>");
			return;
		}
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		PrintWriter writer = new PrintWriter(args[1]);
		String line;

    for (int counter = 1; (line = in.readLine()) != null; ++counter) {
      String[] elements = line.split("\t", 3);
      elements[1] = Normalizer.removeHashFromLink(Normalizer.processTargetLink(elements[1]));
      writer.println(Joiner.on("\t").join(elements[0], elements[1]));
      if (counter % 1000000 == 0) {
      	System.out.println("Processed " + counter + " lines.");
      }
    }
    in.close();
    writer.close();
	}
}
