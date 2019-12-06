package org.ndexbio.service.edgefilter.hiview;

import java.util.TreeSet;

public class TopNEdgeAttributesHolder {
	
	private int limit;
	private TreeSet<FilteredEdgeAttributeEntry> edgeAttrEntries;
	//int counter;
	
	public TopNEdgeAttributesHolder(int sizeLimit) {
		this.limit = sizeLimit;
		this.edgeAttrEntries = new TreeSet <> ();
		//counter = 0;
		
	}
	
	public void addEntry (FilteredEdgeAttributeEntry newEntry) {
		//counter ++;
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
