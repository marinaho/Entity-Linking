package debug;

import iitb.IITBDataset;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FixManualAnnotations {
	public static final String path = "/home/marinah/input/crawledDocs/";
	public static final String manualAnnotationsFile = 
			"/home/marinah/input/manually_found_annot_for_IITB.txt";
	
	public static final String docTag = "DOC: ";
	public static final String nameTag = "NAME: ";
	public static final String offsetTag = "OFFSET: ";
	public static final String urlTag = "URL: en.wikipedia.org/wiki/";
	public static final String contextTag = "CONTEXT: \"";
	
	public static void main(String args[]) throws IOException {
		
		BufferedReader in = new BufferedReader(new FileReader(manualAnnotationsFile));
		String line;
		String doc = "", name = "", context = "", entity = "";
		String content = "", prevdoc = "";
		int offset = -1;

    while ((line = in.readLine()) != null ) {
    	line = line.trim();
    	if (line.startsWith(docTag)) {
    		doc = line.substring(docTag.length());
    	} else if (line.startsWith(nameTag)) {
    		name = line.substring(nameTag.length());
    	} else if (line.startsWith(offsetTag)) {
    		offset = Integer.parseInt(line.substring(offsetTag.length()));
    	} else if (line.startsWith(urlTag)) {
    		entity = line.substring(urlTag.length()).toLowerCase();
    	} else if (line.startsWith(contextTag)) {
    		context = line.substring(contextTag.length(), line.length() - 1);
    		
    		if (!prevdoc.equals(doc)) {
    			content = IITBDataset.getFileContent(path + doc);
    			prevdoc = doc;
    		}
    		
    		String found = content.substring(offset, offset + name.length());
    		if (!found.equals(name)) {
    			System.out.println("Name:" + name + " Entity:" + entity + " Context:" + context + " File:"
    					+ doc + " Offset:" + offset + " Found:" + found);
    			int correctedOffset = content.indexOf(context) + context.indexOf(name);
    			System.out.println("Better offset:" + correctedOffset);
    		} 
    	}
    }
    in.close();

	}
}
