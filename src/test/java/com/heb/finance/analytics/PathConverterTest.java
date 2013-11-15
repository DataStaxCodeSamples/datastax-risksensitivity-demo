package com.heb.finance.analytics;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.heb.finance.analytics.utils.PathConverter;
import com.heb.finance.analytics.utils.PathConverterImpl;

public class PathConverterTest {

	
	@Test
	public void testPathConverter(){
		PathConverter converter = new PathConverterImpl();
		
		
		List<String> parts = converter.convertPathToParts("a/b/c");
				
		Assert.assertTrue(parts.size() == 3);
	}
}
