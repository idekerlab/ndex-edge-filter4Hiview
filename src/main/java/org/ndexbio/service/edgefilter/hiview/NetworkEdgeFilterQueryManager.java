package org.ndexbio.service.edgefilter.hiview;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.ndexbio.cxio.aspects.datamodels.ATTRIBUTE_DATA_TYPE;
import org.ndexbio.cxio.aspects.datamodels.CartesianLayoutElement;
import org.ndexbio.cxio.aspects.datamodels.EdgeAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.EdgesElement;
import org.ndexbio.cxio.aspects.datamodels.NetworkAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.NodeAttributesElement;
import org.ndexbio.cxio.aspects.datamodels.NodesElement;
import org.ndexbio.cxio.core.AspectIterator;
import org.ndexbio.cxio.core.NdexCXNetworkWriter;
import org.ndexbio.cxio.metadata.MetaDataCollection;
import org.ndexbio.cxio.metadata.MetaDataElement;
import org.ndexbio.model.exceptions.BadRequestException;

import com.fasterxml.jackson.core.JsonProcessingException;

public class NetworkEdgeFilterQueryManager {

	static Logger accLogger = Log.getRootLogger();
//  	Log.getRootLogger().info("Embedded Jetty logging started.", new Object[]{});

	private String netId;
	private static final Long consistencyGrp = Long.valueOf(1L);
	private static final String mdeVer = "1.0";
	
	private static String pathPrefix = "/opt/ndex/data/";
	private int edgeLimit;
	private List<FilterCriterion> criteria;
	private boolean returnAllNodes;
	private boolean topN; //whether to return topN edges or the first N edges that meets the criteria. 
	
	public NetworkEdgeFilterQueryManager (String networkId, List<FilterCriterion> criteria, int limit, boolean returnAllNodes, boolean topN) {
	
		this.netId = networkId;
		this.edgeLimit = limit;
		this.criteria = criteria;
		this.returnAllNodes = returnAllNodes;
		this.topN = topN;
	}
	
	public static void setDataFilePathPrefix(String path) {
		pathPrefix = path;
	}
	
	/**
	 * filter query
	 * @param out
	 * @param nodeIds
	 * @throws IOException
	 * @throws BadRequestException 
	 */
	public void filterQuery(OutputStream out) throws IOException {
		
		long t1 = Calendar.getInstance().getTimeInMillis();
		
		Set<Long> edgeIds ; //= new TreeSet<> ();
		Set<Long> nodeIds = new TreeSet<> ();
		
		NdexCXNetworkWriter writer = new NdexCXNetworkWriter(out, true);
		MetaDataCollection md = prepareMetadata() ;
		writer.start();
		writer.writeMetadata(md);
		
		MetaDataCollection postmd = new MetaDataCollection();
		
	
		if (md.getMetaDataElement(EdgeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(EdgeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			
/*			Long previousEdgeId = null;
			boolean criteriaSatisfied = false;
			List<EdgeAttributesElement> attributesHolder = new ArrayList<>(20);
			try (AspectIterator<EdgeAttributesElement> ei = new AspectIterator<>( netId,EdgeAttributesElement.ASPECT_NAME, EdgeAttributesElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					EdgeAttributesElement eAttr = ei.next();
					if ( previousEdgeId == null || ! eAttr.getPropertyOf().equals(previousEdgeId)) {
					   // This is a new group of attributes on a new Id;
					   if ( criteriaSatisfied) {
					      edgeIds.add(previousEdgeId);
					      
					      //write all attributes on this edge out
						  for (EdgeAttributesElement attr: attributesHolder)
							  writer.writeElement(attr);

					      attributesHolder.clear();
					      if (edgeIds.size() > this.edgeLimit) {
                            break;
					      }	  
					   } 
					   
					   previousEdgeId = eAttr.getPropertyOf();
					   //if the current attribute satisfy the criterion, prepare it to be written out;
					   criteriaSatisfied = statisfied( eAttr);
					   if ( criteriaSatisfied)
						   attributesHolder.add(eAttr);
					} else {
					   // another attribute on the same edge	
					   if ( criteriaSatisfied) {
						   criteriaSatisfied = statisfied( eAttr);
						   if ( criteriaSatisfied)
							   attributesHolder.add(eAttr);
						   else 
							   attributesHolder.clear();
					   }	
					}	

				}

			}
			
			if ( attributesHolder .size() >0) {   // last edge also satisfies the criteria. 
				edgeIds.add(previousEdgeId);
				for (EdgeAttributesElement attr: attributesHolder)
					  writer.writeElement(attr);
			} */
			
			if ( topN) { 
			   edgeIds = writeTopNFilteredEdgeAttributes(writer);
			} else 
			   edgeIds = writeFilteredEdgeAttributes(writer);
			
			accLogger.info("Query returned " + writer.getFragmentLength() + " edge attributes.");
			writer.closeFragment();
			writer.endAspectFragment();
		
			MetaDataElement mde = new MetaDataElement(EdgeAttributesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		} else 
			edgeIds = new TreeSet<>();
		
				
		//write out the edges
		if ( md.getMetaDataElement(EdgesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(EdgesElement.ASPECT_NAME);
			writer.openFragment();
            int cnt = 0;
			try (AspectIterator<EdgesElement> ei = new AspectIterator<>( netId,EdgesElement.ASPECT_NAME, EdgesElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					EdgesElement edge = ei.next();
					if ( edgeIds.contains(edge.getId())) {
						writer.writeElement(edge);
						nodeIds.add(edge.getSource());
						nodeIds.add(edge.getTarget());
						cnt++;
						if(cnt == edgeIds.size()) {
								break;
						}
					} 
				}
			}
			writer.closeFragment();
			writer.endAspectFragment();
		}
		
		
		//write nodes
		writer.startAspectFragment(NodesElement.ASPECT_NAME);
		writer.openFragment();
        long cnt = 0;
        long maxNodeId = 0;
		try (AspectIterator<NodesElement> ei = new AspectIterator<>(netId, NodesElement.ASPECT_NAME, NodesElement.class, pathPrefix)) {
            while (ei.hasNext()) {
				NodesElement node = ei.next();
				if (returnAllNodes || nodeIds.contains(Long.valueOf(node.getId()))) {
						writer.writeElement(node);
						cnt ++;
						if ( node.getId() > maxNodeId )
							maxNodeId = node.getId();
						if ( !returnAllNodes && cnt == nodeIds.size())
							break;
				}
			}
		}
		writer.closeFragment();
		writer.endAspectFragment();
		
		if ( cnt > 0) {
			MetaDataElement mde1 = new MetaDataElement(NodesElement.ASPECT_NAME,mdeVer);
			mde1.setElementCount(Long.valueOf(cnt));
			mde1.setIdCounter(Long.valueOf(maxNodeId));
			postmd.add(mde1);
		}
		
		
		if  (edgeIds.size()>0) {
			MetaDataElement mde = new MetaDataElement(EdgesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount((long)edgeIds.size());
			mde.setIdCounter(Collections.max(edgeIds));
			postmd.add(mde);
		}
		
		writeOtherAspectsForSubnetwork(nodeIds, writer, md, postmd);
		
		writer.writeMetadata(postmd);
		writer.end();
		long t2 = Calendar.getInstance().getTimeInMillis();

		accLogger.info("Total " + (t2-t1)/1000f + " seconds. Returned " + edgeIds.size() + " edges and " + nodeIds.size() + " nodes.",
				new Object[]{});
	}


	private Set<Long> writeTopNFilteredEdgeAttributes (NdexCXNetworkWriter writer) throws IOException {

		TopNEdgeAttributesHolder attrHolder = new TopNEdgeAttributesHolder ( this.edgeLimit);		
		EdgeAttributesElement[] keyEntryHolder = new EdgeAttributesElement[1];
		
		Long previousEdgeId = null;
		boolean criteriaSatisfied = false;
		List<EdgeAttributesElement> attributesHolder = new ArrayList<>(20);
		try (AspectIterator<EdgeAttributesElement> ei = new AspectIterator<>( netId,EdgeAttributesElement.ASPECT_NAME, EdgeAttributesElement.class, pathPrefix)) {
			while (ei.hasNext()) {
				EdgeAttributesElement eAttr = ei.next();
				if ( previousEdgeId == null || ! eAttr.getPropertyOf().equals(previousEdgeId)) {
				   // This is a new group of attributes on a new Id;
				   if ( criteriaSatisfied) {
				      
					   
					   FilteredEdgeAttributeEntry e = new FilteredEdgeAttributeEntry(attributesHolder, keyEntryHolder[0], previousEdgeId);
					   
					   attrHolder.addEntry(e);
					   
					 /*  edgeIds.add(previousEdgeId);
				      
				      //write all attributes on this edge out
					  for (EdgeAttributesElement attr: attributesHolder)
						  writer.writeElement(attr);
					  

				      attributesHolder.clear(); */
					  attributesHolder = new ArrayList<>(20);
					  keyEntryHolder[0] = null;
				      
				   } 
				   
				   previousEdgeId = eAttr.getPropertyOf();
				   //if the current attribute satisfy the criterion, prepare it to be written out;
				   criteriaSatisfied = topNStatisfied( eAttr, keyEntryHolder);
				   if ( criteriaSatisfied)
					   attributesHolder.add(eAttr);
				} else {
				   // another attribute on the same edge	
				   if ( criteriaSatisfied) {
					   criteriaSatisfied = topNStatisfied( eAttr, keyEntryHolder);
					   if ( criteriaSatisfied)
						   attributesHolder.add(eAttr);
					   else {
						   attributesHolder.clear();
						   keyEntryHolder[0]=null;
					   }   
				   }	
				}	

			}

		}
		
		if ( attributesHolder .size() >0) {   // last edge also satisfies the criteria. 
			
			attrHolder.addEntry(new FilteredEdgeAttributeEntry(attributesHolder, keyEntryHolder[0], previousEdgeId));
			/*edgeIds.add(previousEdgeId);
			for (EdgeAttributesElement attr: attributesHolder)
				  writer.writeElement(attr);*/
		}
		
		Set<Long> edgeIds = new TreeSet<> ();
		
		for (FilteredEdgeAttributeEntry entry: attrHolder.getEntries()) {
			for (EdgeAttributesElement attrElmt: entry.getAttributes()) {
				writer.writeElement(attrElmt);
			}
			edgeIds.add(entry.getEdgeId());
		}

		return edgeIds;
	}
	
	private Set<Long> writeFilteredEdgeAttributes (NdexCXNetworkWriter writer) throws IOException {
		Set<Long> edgeIds = new TreeSet<> ();
		
		Long previousEdgeId = null;
		boolean criteriaSatisfied = false;
		List<EdgeAttributesElement> attributesHolder = new ArrayList<>(20);
		try (AspectIterator<EdgeAttributesElement> ei = new AspectIterator<>( netId,EdgeAttributesElement.ASPECT_NAME, EdgeAttributesElement.class, pathPrefix)) {
			while (ei.hasNext()) {
				EdgeAttributesElement eAttr = ei.next();
				if ( previousEdgeId == null || ! eAttr.getPropertyOf().equals(previousEdgeId)) {
				   // This is a new group of attributes on a new Id;
				   if ( criteriaSatisfied) {
				      edgeIds.add(previousEdgeId);
				      
				      //write all attributes on this edge out
					  for (EdgeAttributesElement attr: attributesHolder)
						  writer.writeElement(attr);

				      attributesHolder.clear();
				      if (edgeIds.size() > this.edgeLimit) {
                        break;
				      }	  
				   } 
				   
				   previousEdgeId = eAttr.getPropertyOf();
				   //if the current attribute satisfy the criterion, prepare it to be written out;
				   criteriaSatisfied = statisfied( eAttr);
				   if ( criteriaSatisfied)
					   attributesHolder.add(eAttr);
				} else {
				   // another attribute on the same edge	
				   if ( criteriaSatisfied) {
					   criteriaSatisfied = statisfied( eAttr);
					   if ( criteriaSatisfied)
						   attributesHolder.add(eAttr);
					   else 
						   attributesHolder.clear();
				   }	
				}	

			}

		}
		
		if ( attributesHolder .size() >0) {   // last edge also satisfies the criteria. 
			edgeIds.add(previousEdgeId);
			for (EdgeAttributesElement attr: attributesHolder)
				  writer.writeElement(attr);
		}
		
		return edgeIds;
	}
	
	   private boolean topNStatisfied (EdgeAttributesElement e, EdgeAttributesElement[] keyEntryHolder) {
	       for ( FilterCriterion filter : criteria) {
	    	   if ( e.getName().equals(filter.getName())) {
	    		   ATTRIBUTE_DATA_TYPE t = e.getDataType();
	    		   switch (t) {
	    		   case DOUBLE: {
	    			   Double d = Double.valueOf(e.getValue());
	    			   Double condValue = Double.valueOf(filter.getValue());
	    			   boolean satisfied = compare(d, condValue, filter.getOperator());
	    			   if (!satisfied)
	    				   return false;
	    			   break;
	    		   }
	    		   case LONG: {
	    			   Long d = Long.valueOf(e.getValue());
	    			   Long condValue = Long.valueOf(filter.getValue());
	    			   boolean satisfied = compare(d, condValue, filter.getOperator());
	    			   if (!satisfied)
	    				   return false;
	    			   break;
	    		   }	   
	    		   case INTEGER:{
	    			   Integer d = Integer.valueOf(e.getValue());
	    			   Integer condValue = Integer.valueOf(filter.getValue());
	    			   boolean satisfied = compare(d, condValue, filter.getOperator());
	    			   if (!satisfied)
	    				   return false;
	    			   break;
	    		   }
	    		   default: 
	    			   return false;
	    		   }
    			  
	    		   keyEntryHolder[0] = e;
	    	   }
	       }
	       return true;	
	    } 
	    
	
    private boolean statisfied (EdgeAttributesElement e) {
       for ( FilterCriterion filter : criteria) {
    	   if ( e.getName().equals(filter.getName())) {
    		   ATTRIBUTE_DATA_TYPE t = e.getDataType();
    		   switch (t) {
    		   case DOUBLE: {
    			   Double d = Double.valueOf(e.getValue());
    			   Double condValue = Double.valueOf(filter.getValue());
    			   boolean satisfied = compare(d, condValue, filter.getOperator());
    			   if (!satisfied)
    				   return false;
    			   break;
    		   }
    		   case LONG: {
    			   Long d = Long.valueOf(e.getValue());
    			   Long condValue = Long.valueOf(filter.getValue());
    			   boolean satisfied = compare(d, condValue, filter.getOperator());
    			   if (!satisfied)
    				   return false;
    			   break;
    		   }	   
    		   case INTEGER:{
    			   Integer d = Integer.valueOf(e.getValue());
    			   Integer condValue = Integer.valueOf(filter.getValue());
    			   boolean satisfied = compare(d, condValue, filter.getOperator());
    			   if (!satisfied)
    				   return false;
    			   break;
    		   }
    		   case STRING: {
    			   boolean satisfied = compare(e.getValue(), filter.getValue(), filter.getOperator());
    			   if (!satisfied)
    				   return false;
    			   break;
    		   }
    		   case BOOLEAN:{
    			   Boolean d = Boolean.valueOf(e.getValue());
    			   Boolean condValue = Boolean.valueOf(filter.getValue());
    			   boolean satisfied = compare(d, condValue, filter.getOperator());
    			   if (!satisfied)
    				   return false;
    			   break;
    		   }
    		   default: 
    			   break;
    		   }
    	   }
       }
       return true;	
    } 
    
    private static <T> boolean compare(Comparable<T> op1, Comparable<T> op2, String operator) {
    	int r = op1.compareTo((T) op2);
    	if (operator.equals(">"))
    		return r > 0;
    	if (operator.equals("<"))
    		return r < 0;
    	if ( operator.equals("="))
    		return r == 0;
    	if (operator.equals("!="))
    		return r != 0;
    	return true;
    }
    
	private void writeOtherAspectsForSubnetwork(Set<Long> nodeIds, NdexCXNetworkWriter writer,
			MetaDataCollection md, MetaDataCollection postmd) throws IOException, JsonProcessingException {
		//process node attribute aspect
		if (md.getMetaDataElement(NodeAttributesElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(NodeAttributesElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<NodeAttributesElement> ei = new AspectIterator<>(netId, NodeAttributesElement.ASPECT_NAME, NodeAttributesElement.class, pathPrefix)) {
					while (ei.hasNext()) {
						NodeAttributesElement nodeAttr = ei.next();
						if (nodeIds.contains(nodeAttr.getPropertyOf())) {
								writer.writeElement(nodeAttr);
						}
					}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(NodeAttributesElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
		
		//write networkAttributes
		writer.startAspectFragment(NetworkAttributesElement.ASPECT_NAME);
		writer.openFragment();
		if (md.getMetaDataElement(NetworkAttributesElement.ASPECT_NAME) != null) {
			try (AspectIterator<NetworkAttributesElement> ei = new AspectIterator<>(netId,NetworkAttributesElement.ASPECT_NAME, NetworkAttributesElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					NetworkAttributesElement attr = ei.next();
					writer.writeElement(attr);
				}
			}
		}
			
		writer.closeFragment();
		writer.endAspectFragment();
		MetaDataElement mde2 = new MetaDataElement(NetworkAttributesElement.ASPECT_NAME, mdeVer);
		mde2.setElementCount( writer.getFragmentLength());
		postmd.add(mde2);

		//process cartesianLayout
		if (md.getMetaDataElement(CartesianLayoutElement.ASPECT_NAME) != null) {
			writer.startAspectFragment(CartesianLayoutElement.ASPECT_NAME);
			writer.openFragment();
			try (AspectIterator<CartesianLayoutElement> ei = new AspectIterator<>(netId, CartesianLayoutElement.ASPECT_NAME, CartesianLayoutElement.class, pathPrefix)) {
				while (ei.hasNext()) {
					CartesianLayoutElement nodeCoord = ei.next();
						if (returnAllNodes || nodeIds.contains(nodeCoord.getNode())) {
							writer.writeElement(nodeCoord);
						}
				}
			}
			writer.closeFragment();
			writer.endAspectFragment();
			MetaDataElement mde = new MetaDataElement(CartesianLayoutElement.ASPECT_NAME,mdeVer);
			mde.setElementCount(writer.getFragmentLength());
			postmd.add(mde);
		}	
	}
	
	
	private MetaDataCollection prepareMetadata() {
		MetaDataCollection md = new MetaDataCollection();
		File dir = new File(pathPrefix+netId+"/aspects");
		  File[] directoryListing = dir.listFiles();
		  for (File child : directoryListing) {
			  String aspName = child.getName();
			  MetaDataElement e;
			  e = new MetaDataElement (aspName, mdeVer);
			  e.setConsistencyGroup(consistencyGrp);
			  md.add(e);			  
		  }
		  return md;
	}
	
}
