package org.ndexbio.service.edgefilter.hiview;

import java.util.List;

import org.ndexbio.cxio.aspects.datamodels.ATTRIBUTE_DATA_TYPE;
import org.ndexbio.cxio.aspects.datamodels.EdgeAttributesElement;

public class FilteredEdgeAttributeEntry implements Comparable <FilteredEdgeAttributeEntry> {
	
	private List<EdgeAttributesElement> attributes;
	private EdgeAttributesElement keyColumn; // position of the key attributes.
	Long edgeId;
	
	public FilteredEdgeAttributeEntry(List<EdgeAttributesElement> attrs, EdgeAttributesElement key, Long edgeId) {
		this.attributes = attrs;
		this.keyColumn = key;
		this.edgeId = edgeId;
	}
	
	public EdgeAttributesElement getKeyColumn() { return keyColumn; }
	public List<EdgeAttributesElement> getAttributes() {return attributes;}
	public Long getEdgeId() { return edgeId;}
	
	@Override
	public int compareTo(FilteredEdgeAttributeEntry o) {
		ATTRIBUTE_DATA_TYPE t = keyColumn.getDataType();
		   switch (t) {
		   case DOUBLE: {
			   Double d = Double.valueOf(keyColumn.getValue());
			   Double d2 = Double.valueOf(o.getKeyColumn().getValue());
			   return d.compareTo(d2);
			   
		   }
		   case LONG: {
			   Long d = Long.valueOf(keyColumn.getValue());
			   Long condValue = Long.valueOf(o.getKeyColumn().getValue());
			   return d.compareTo(condValue);
		   }	   
		   case INTEGER:{
			   Integer d = Integer.valueOf(keyColumn.getValue());
			   Integer condValue = Integer.valueOf(o.getKeyColumn().getValue());
			   return d.compareTo(condValue);
		   }
		   default: 
			   return 0;
		   }
	}
	
}
