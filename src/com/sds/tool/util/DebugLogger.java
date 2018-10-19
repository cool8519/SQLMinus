package com.sds.tool.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DebugLogger {

    public static final int ERROR = 0;
    public static final int WARN = 1;
    public static final int INFO = 2;
    public static final int DEBUG = 3;

	private static SimpleDateFormat format = new SimpleDateFormat("yyyy.MM.dd/HH:mm:ss");
    private static String fileName = null;
    private static File f = null;
    private static FileWriter fw = null;
    private static BufferedWriter bw = null;
    private static boolean consoleSilent = true;
    private static boolean fileSilent = false;
    private static int showLevel = DEBUG;

    
    public static String getFileName() {
        return fileName;
    }

    public static boolean getConsoleSilent() {
        return consoleSilent;
    }


    public static void setConsoleSilent(boolean s) {
    	consoleSilent = s;
    }

    
    public static boolean getFileSilent() {
        return fileSilent;
    }


    public static void setFileSilent(boolean s) {
    	fileSilent = s;
    }


    public static void setShowLevel(int level) {
        showLevel = level;
    }


    public static void setFileName(String fname, boolean append) {
        try {
            if(fileName != null) {
                setFileOff();
            }
            fileName = fname;
            f = new File(fileName);
            fw = new FileWriter(f, append);
            bw = new BufferedWriter(fw);
        } catch(Exception e) {}
    }


    public static void setFileOff() {
        try {
            if(fileName != null) {
                bw.flush();
                bw.close();
            }
            fileName = null;
        } catch(Exception e) {}
    }


    public static void log(Object msg) {
        log(msg, ERROR);
    }


    public static void logln(Object msg) {
        logln(msg, ERROR);
    }


    public static void log(Object msg, int level) {
        if(level <= showLevel) {
            log(getFormattedMessage(level, msg), null, false);
        }
    }


    public static void logln(Object msg, int level) {
        if(level <= showLevel) {
            log(getFormattedMessage(level, msg), null, true);
        }
    }

    public static void logln(Object msg, Throwable t, int level) {
        if(level <= showLevel) {
            log(getFormattedMessage(level, msg), t, true);
        }
    }

    public static void log(Object msg, Throwable t, boolean newline) {
        try {
    		logOnlyConsole(msg, newline);
    		logOnlyFile(msg, t, newline);
        } catch(Exception e) {}
    }

    public static void logOnlyConsole(Object msg, boolean newline) {
    	if(!consoleSilent) {
	        if(newline) {
	            System.out.println(msg);
	        } else {
	            System.out.print(msg);
	        }
    	}
    }
    

    public static void logOnlyFile(Object msg, Throwable t, boolean newline) {
    	if(!fileSilent) {
	        String str = "";
	        try {
	            if(fileName != null) {
	                str = (msg instanceof String)?(String)msg:msg.toString();
	                str = str.replaceAll("\r\n", "\n").replaceAll("\n", "\r\n");
	                if(newline) {
	                    bw.write(str);
	                    bw.newLine();
	                } else {
	                    bw.write(str);
	                    if(t != null) {
	                    	bw.newLine();
	                    }
	                }
	    	        if(t != null) {
	    	            t.printStackTrace(new PrintWriter(bw));
	    	        }
	                bw.flush();
	            }
	        } catch(Exception e) {}
    	}
    }

    
    private static String getFormattedMessage(int level, Object msg) {
		String timeStr = format.format(new Date());
		String levelStr = "[" + level + "]";
		String threadStr = "[" + Thread.currentThread().getName() + "]";
		StringBuilder logstr = new StringBuilder();
		logstr.append("[").append(timeStr).append("]").append(levelStr).append(threadStr).append(" ");
		logstr.append(msg.toString());
		return logstr.toString();    	
    }
    
}