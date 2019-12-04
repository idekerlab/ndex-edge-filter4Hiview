package org.ndexbio.service.edgefilter.hiview;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;

import org.jboss.resteasy.plugins.interceptors.CorsFilter;

public class EdgeFilterQueryApplication extends Application {

    	 private final Set<Object> _providers = new HashSet<>();

	  public EdgeFilterQueryApplication() {
		  _providers.add(new MessageResource());
		 
	        _providers.add(new NdexDefaultResponseFilter());
	        _providers.add(new DefaultExceptionMapper());
	        _providers.add(new BadRequestExceptionMapper());
	        _providers.add(new ForbiddenExceptionMapper());
	        

	        CorsFilter corsFilter = new CorsFilter();
	        corsFilter.getAllowedOrigins().add("*");
	        corsFilter.setAllowCredentials(true);
	        _providers.add( corsFilter);

	  }

	  @Override
	  public Set<Object> getSingletons() {
	 //   HashSet<Object> set = new HashSet<>();
	 //   set.add(new MessageResource());
	    return _providers;
	  }
}