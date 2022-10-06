package org.ndexbio.service.edgefilter.hiview;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.ndexbio.model.exceptions.ForbiddenOperationException;
import org.ndexbio.model.exceptions.NdexException;

@Path("/v1")
public class MessageResource {

  @SuppressWarnings("static-method")
  @GET
  @Path("/status")
  @Produces("application/json")
  public Map<String,String> printMessage() {
     Map<String,String> result = new HashMap<>();
     result.put("status", "online");
     return result;
  }
  
  
 
  
	@POST
	@Path("/mytest")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)

    public static String mytest (@Context HttpServletRequest request/*, InputStream in*/) {
	   return "abc";
	}
  
	@POST
	@Path("/myTest2")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)

    public static String myTest2 (@Context HttpServletRequest request/*, InputStream in*/) {
	   return "abc";
	}

	
	@POST
	@Path("/network/{networkId}/topNEdgeFilter")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response  topNEdgeFilterQuery(
			@PathParam("networkId") final String networkIdStr,
			@DefaultValue("1000") @QueryParam("limit") int limit,	
			@DefaultValue("true") @QueryParam("returnAllNodes") boolean returnAllNodes,			
			final List<FilterCriterion> queryParameters
			) throws IOException, NdexException {
		
		if (queryParameters.size()!= 1 ) {
			throw new ForbiddenOperationException("This function only takes one filter criterion.");
		}
		
		FilterCriterion condition = queryParameters.get(0);
		if (! condition.getOperator().equals(">") )
			throw new ForbiddenOperationException("This function only supports '>' operator.");
		
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			in.close();
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new EdgeFilterQueryWriterThread(out,networkIdStr,queryParameters, limit, returnAllNodes, true).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}	

	@POST
	@Path("/network/{networkId}/rankededgefilter")  // same function as above, for debugging
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response  topNEdgeFilterQuery2(
			@PathParam("networkId") final String networkIdStr,
			@DefaultValue("1000") @QueryParam("limit") int limit,	
			@DefaultValue("true") @QueryParam("returnAllNodes") boolean returnAllNodes,			
			final List<FilterCriterion> queryParameters
			) throws IOException, NdexException {
		
		if (queryParameters.size()!= 1 ) {
			throw new ForbiddenOperationException("This function only takes one filter criterion.");
		}
		
		FilterCriterion condition = queryParameters.get(0);
		if (! condition.getOperator().equals(">") )
			throw new ForbiddenOperationException("This function only supports '>' operator.");
		
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			in.close();
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new EdgeFilterQueryWriterThread(out,networkIdStr,queryParameters, limit, returnAllNodes, true).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}	

	
	@POST
	@Path("/network/{networkId}/edgefilter")
	@Produces("application/json")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response  firstNEdgeFilterQuery(
			@PathParam("networkId") final String networkIdStr,
			@DefaultValue("1000") @QueryParam("limit") int limit,	
			@DefaultValue("true") @QueryParam("returnAllNodes") boolean returnAllNodes,			
			final List<FilterCriterion> queryParameters
			) throws IOException, NdexException {
		
		PipedInputStream in = new PipedInputStream();
		 
		PipedOutputStream out;
		
 		try {
			out = new PipedOutputStream(in);
		} catch (IOException e) {
			in.close();
			throw new NdexException("IOExcetion when creating the piped output stream: "+ e.getMessage());
		}
		
		new EdgeFilterQueryWriterThread(out,networkIdStr,queryParameters, limit, returnAllNodes, false).start();
		//setZipFlag();
		return Response.ok().type(MediaType.APPLICATION_JSON_TYPE).entity(in).build();
		

	}
	
	private class EdgeFilterQueryWriterThread extends Thread {
		private OutputStream o;
		private NetworkEdgeFilterQueryManager queryManager;
		
		public EdgeFilterQueryWriterThread (OutputStream out, String networkUUID, List<FilterCriterion> query, int limit, boolean returnAllNodes, boolean topN) {
			o = out;
			queryManager = new NetworkEdgeFilterQueryManager (networkUUID, query, limit, returnAllNodes, topN);	
		}
		
		@Override
		public void run() {
			try {
				queryManager.filterQuery(o);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		//		o.write("error:" + e.getMessage());
			} finally {
				try {
					o.flush();
					o.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
}	