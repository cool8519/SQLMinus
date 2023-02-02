package com.sds.tool.util;

public class ClassLoaderUtil {

    public static String getJarPathFromClass(ClassLoader cl, String classname) {
    	if(cl == null || classname == null || classname.trim().equals("")) {
    		return null;
    	}
    	if(!classname.endsWith(".class")) {
    		classname = classname.replace('.', '/') + ".class";
    	}
    	String location = cl.getResource(classname).getFile();
    	int fromIdx = location.indexOf(':') + 1;
    	return location.substring(fromIdx, location.indexOf('!',fromIdx));
    }

    public static String getPathFromClass(ClassLoader cl, String classname) {
    	if(cl == null || classname == null || classname.trim().equals("")) {
    		return null;
    	}
    	if(!classname.endsWith(".class")) {
    		classname = classname.replace('.', '/') + ".class";
    	}
    	return cl.getResource(classname).getFile();
    }

}
