package com.heb.finance.analytics.utils;

import java.util.ArrayList;
import java.util.List;

public class PathConverterImpl implements PathConverter{

	private static final String DIVIDER = "/";

	@Override
	public List<String> convertPathToParts(String path) {
		
		if (path == null || path.isEmpty()){
			return new ArrayList<String>();			
		}

		List<String> parts = new ArrayList<String>();	
		String subPath = path;

		while (subPath.contains(DIVIDER)) {
			parts.add(subPath);

			subPath = subPath.substring(0, subPath.lastIndexOf(DIVIDER));
		}
		parts.add(subPath);
		
		return parts;
	}
}
