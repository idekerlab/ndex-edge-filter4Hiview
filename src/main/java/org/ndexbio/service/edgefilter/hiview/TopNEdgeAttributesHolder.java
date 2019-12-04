package org.ndexbio.service.edgefilter.hiview;

import java.util.TreeSet;

public class TopNEdgeAttributesHolder {
	
	private int limit;
	private TreeSet<FilteredEdgeAttributeEntry> edgeAttrEntries;
	
	public TopNEdgeAttributesHolder(int sizeLimit) {
		this.limit = sizeLimit;
		this.edgeAttrEntries = new TreeSet <> ();
		
	}
	
	public void addEntry (FilteredEdgeAttributeEntry newEntry) {
		if ( edgeAttrEntries.size() < limit ) {
			edgeAttrEntries.add(newEntry);
			return;
		}
		
		if ( newEntry.compareTo(edgeAttrEntries.first()) >0 ) {
			edgeAttrEntries.pollFirst();
			edgeAttrEntries.add(newEntry);
		}
		
	}
	
	public TreeSet<FilteredEdgeAttributeEntry> getEntries() {return this.edgeAttrEntries;}
	
}
