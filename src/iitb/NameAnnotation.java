package iitb;

import index.TitleIDsIndex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import md.Mention;

/**
 * Annotation given by name, entity and file. The position in the file (offset) does not matter.
 */
public class NameAnnotation {
	private String name;
	private int entity;
	private String filename;
	
	public NameAnnotation(String name, int entity, String filename) {
		this.name = name;
		this.entity = entity;
		this.filename = filename;
	}
	
	public String getName() {
		return name;
	}
	
	public int getEntity() {
		return entity;
	}
	
	public String getFilename() {
		return filename;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}
		NameAnnotation other = (NameAnnotation) obj;
		if (name.equals(other.getName()) && filename.equals(other.getFilename()) && 
				entity == other.getEntity()){
			return true;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return name.hashCode() + entity + filename.hashCode();
	}
	
	@Override
	public String toString() {
		return "Name:" + name + " Entity:" + entity + " Filename" + filename;
	}
	
	public static String outputAnnotations(List<NameAnnotation> annotations, 
			TitleIDsIndex titleIdsIndex) {
		StringBuilder result = new StringBuilder();
		for (NameAnnotation annotation: annotations) {
			result.append("Name:" + annotation.getName() + " Entity id:" + annotation.getEntity() + 
					" Entity:" + titleIdsIndex.get(annotation.getEntity()) + " File:" + 
					annotation.getFilename() + "\n");
		}
		return result.toString();
	}
	
	public static Set<NameAnnotation> getSet(HashMap<Mention, Integer> solution, String filename) {
		Set<NameAnnotation> nameAnnotations = new HashSet<NameAnnotation>();
		for (Map.Entry<Mention, Integer> entry: solution.entrySet()) {
			Mention mention = entry.getKey();
			int entity = entry.getValue();
			nameAnnotations.add(new NameAnnotation(mention.getOriginalNgram(), entity, filename));
		}
		return nameAnnotations;
	}
}
