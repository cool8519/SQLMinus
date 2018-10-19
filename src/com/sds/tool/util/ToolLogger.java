package com.sds.tool.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;


public class ToolLogger {

    public static final int BUFFER_SIZE = 16384;
    public static final int RESULT = 0;
    public static final int ERROR = 1;
    public static final int INFO = 2;

    private static String spoolName = null;
    private static File f = null;
    private static FileWriter fw = null;
    private static BufferedWriter bw = null;
    private static boolean silent = false;
    private static int showLevel = ERROR;


    public static String getSpoolName() {
        return spoolName;
    }


    public static boolean getSilent() {
        return silent;
    }


    public static void setSilent(boolean s) {
        silent = s;
    }


    public static void setShowLevel(int level) {
        showLevel = level;
    }


    public static void setSpoolName(String fname, boolean append) {
        try {
            if(spoolName != null) {
                setSpoolOff();
            }
            spoolName = fname;
            f = new File(spoolName);
            fw = new FileWriter(f, append);
            bw = new BufferedWriter(fw);
            //bw = new BufferedWriter(fw, BUFFER_SIZE);
        } catch(Exception e) {}
    }


    public static void setSpoolOff() {
        try {
            if(spoolName != null) {
                bw.flush();
                bw.close();
            }
            spoolName = null;
        } catch(Exception e) {}
    }


    public static void log(Object msg) {
        log(msg, RESULT);
    }


    public static void logln(Object msg) {
        logln(msg, RESULT);
    }


    public static void log(Object msg, int level) {
        if(!silent || (silent && level <= showLevel)) {
            log(msg, false);
        }
    }


    public static void logln(Object msg, int level) {
        if(!silent || (silent && level <= showLevel)) {
            log(msg, true);
        }
    }


    public static void log(Object msg, boolean newline) {
        try {
            if(newline) {
                System.out.println(msg);
            } else {
                System.out.print(msg);
            }

            logOnlySpool(msg, newline);
        } catch(Exception e) {}
    }


    public static void logOnlySpool(Object msg, boolean newline) {
        String str = "";
        try {
            if(spoolName != null) {
                str = (msg instanceof String)?(String)msg:msg.toString();
                str = str.replaceAll("\r\n", "\n").replaceAll("\n", "\r\n");
                if(newline) {
                    bw.write(str);
                    bw.newLine();
                } else {
                    bw.write(str);
                }
            }
        } catch(Exception e) {}
    }

}