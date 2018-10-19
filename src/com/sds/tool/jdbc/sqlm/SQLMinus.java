package com.sds.tool.jdbc.sqlm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sds.tool.jdbc.sqlm.util.RetainableConnection;
import com.sds.tool.jdbc.sqlm.util.Util;
import com.sds.tool.util.DebugLogger;
import com.sds.tool.util.StringUtil;
import com.sds.tool.util.ToolLogger;


public class SQLMinus {

    private static final String APPLY   = "APPLY";
    private static final String FORMAT  = "FORMAT";
    private static final String HEADING = "HEADING";
    private static final String WRAPPED = "WRAPPED";
    private static final String PRODUCT = "SQL*Minus";
    private static final String VERSION = "1.2.0";
    private static final String DEBUG_FILE = "./sqlm_dbg.log";

    private DBInfo dbinfo = new DBInfo();
    private RetainableConnection rcon = null;
    private ResultSet rs = null;
    private String cmd = "";
    private String user = "";
    private String pass = "";
    private String dbname = "";
    private String action = "";
    private String prev = "";
    private String scriptName = "";
    private long start = 0L;
    private int pageSize = 24;
    private int lineSize = 80;
    private int checkSize = 1000;
    private int timeOut = 60;
    private int checkConnIdleTime = 5;
    private int checkConnMethod = RetainableConnection.CheckType.ALL;
    private boolean showHead = true;
    private boolean showTiming = false;
    private boolean showTime = false;
    private boolean scanVar = true;
    private boolean checkResultSet = true;
    private boolean commitOnExit = true;
    private boolean loop = true;
    private boolean scriptOnStart = false;
    private Hashtable<String,Properties> setCols = new Hashtable<String,Properties>();
    private Hashtable<String,String> defines = new Hashtable<String,String>();
    private List<Hashtable<String,Savepoint>> saves = new ArrayList<Hashtable<String,Savepoint>>();

    public static String sqlbuffer = "";


    public SQLMinus() {}


    public static void main(String args[]) {
        SQLMinus sm = new SQLMinus();
        sm.start(args);
    }


    public void start(String params[]) {

    	Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
    	    public void run() {
                DebugLogger.logln("Stopping SQLMinus.", DebugLogger.WARN);
                ToolLogger.setSpoolOff();
                DebugLogger.setFileOff();
    	    }
    	}));

        LinkedList<String> ll = new LinkedList<String>();

        try {

            String query = "";
            String tm = "";


            if(params.length > 0) {

                String opt = (params[0].startsWith("/")||params[0].startsWith("-"))?params[0].substring(1):params[0];
                if(opt.equalsIgnoreCase("help") || opt.equalsIgnoreCase("h") || opt.equals("?")) {
                    printMainHelp();
                } else if(opt.equalsIgnoreCase("version") || opt.equalsIgnoreCase("v")) {
                    printProgramVersion();
                } else {
                	int idx = -1;
                    String name = "";
                    String value = "";
                    boolean silent = false;
                    if(opt.equalsIgnoreCase("silent") || opt.equalsIgnoreCase("s")) {
                        ToolLogger.setSilent(true);
                        silent = true;
                    }
                    if(opt.equalsIgnoreCase("debug") || opt.equalsIgnoreCase("d")) {
                        DebugLogger.setFileName(DEBUG_FILE, true);
                    }

                    DebugLogger.logln("Starting SQLMinus.", DebugLogger.WARN);
                    DebugLogger.logln("Strart options: " + StringUtil.arrayToString(params,", "), DebugLogger.WARN);
                    
                    for(int i = ((silent)?1:0); i < params.length; i++) {
                        idx = params[i].indexOf("=");
                        if(idx < 0) {
                        	name = "";
                            value = params[i];
                        } else {
                            name = params[i].substring(0,idx);
                            value = params[i].substring(idx+1);
                        }

                        if(name.equalsIgnoreCase("driver")) {
                        	String[] driverPaths = value.trim().split(";");
                        	dbinfo.setDrivers(Arrays.asList(driverPaths));
                        } else if(name.equalsIgnoreCase("url")) {
                            if(dbinfo.setConnectionURL(value) == false) {
                                printErrorOnStart("ERROR: Invalid ConnectionURL\n");
                            }
                        } else if(name.equalsIgnoreCase("logon")) {
                            int idx2 = value.indexOf("/");
                            if(idx2 < 0) {
                                dbinfo.setUsername(value);
                            } else {
                                dbinfo.setUsername(value.substring(0,idx2));
                                dbinfo.setPassword(value.substring(idx2+1));
                            }
                        } else if(name.equalsIgnoreCase("encoding")) {
                            int idx2 = value.indexOf("/");
                            if(idx2 < 0) {
                                dbinfo.setInputCharset(value);
                            } else {
                                dbinfo.setInputCharset(value.substring(0,idx2));
                                dbinfo.setOutputCharset(value.substring(idx2+1));
                            }
                        } else if(name.equalsIgnoreCase("script")) {
                            scriptOnStart = true;
                            scriptName = value;
                            DebugLogger.logln("Set script : " + value, DebugLogger.WARN);
                        } else if(value.equalsIgnoreCase("-d") || value.equalsIgnoreCase("/d") || value.equalsIgnoreCase("-debug") || value.equalsIgnoreCase("/debug")) {
                        	continue;
                        } else {
                            printErrorOnStart("ERROR: Invalid parameter\n");
                        }
                    }
                }

            }

            dbinfo.getInfo();
            dbinfo.loadJDBCDriver();

            createConnection(true, false);

            while(loop) {
                if(scriptOnStart) {
                    cmd = "@" + scriptName;
                    action = getAction(cmd);
                    scriptOnStart = false;
                } else {
                    cmd = "";
                    tm = (showTime)?(Util.getCurrentTime()+" "):"";
                    action = readSQL(tm + "SQL> ").trim();
                    ll.clear();
                }
                processAction(action, query, tm, ll);
            }
            
            DebugLogger.logln("End of command loop.", DebugLogger.WARN);

        } catch(Exception e) {
            logln(e.getMessage().trim(), ToolLogger.ERROR);
	        DebugLogger.logln("Exception occurred in start().", e, DebugLogger.ERROR);
        } finally {
            closeConnection();        	
        }

    }


    private void closeConnection() {

        try {
            if( rs != null ) try { rs.close(); } catch(Exception e) {};
            if( rcon != null ) {
                try {
                    if(commitOnExit) {
                        rcon.commit();
                        logln("\nCommit complete.\n", ToolLogger.INFO);
                    } else {
                        rcon.rollback();
                        logln("\nRollback complete.\n", ToolLogger.INFO);
                    }
                    log("Disconnected from " + rcon.getMetaData().getDatabaseProductVersion() + "\n", ToolLogger.INFO);
                } catch(Exception e) {};
                try { rcon.setAutoCommit(true); } catch(Exception e) {};
                try { rcon.close(); } catch(Exception e) {};
                rcon = null;
            }
        } catch (Exception e) {
            logln(e.getMessage().trim(), ToolLogger.ERROR);
        }

    }


    private void createConnection(boolean withCurrentInfo) throws Exception {
        createConnection(false, withCurrentInfo);
    }


    private void createConnection(boolean first, boolean withCurrentInfo) throws Exception {

        String dbname_temp = (dbname.length()>0)?dbname:dbinfo.getDBName();
        if(!first && !withCurrentInfo) {
            dbinfo.resetInfo(user, pass, dbname_temp);
        }
        user = pass = dbname = "";

        start = System.currentTimeMillis();

        try {
            if(first) {
                logln("JAVA Encoding: input=" + dbinfo.getInputCharset() + ", output=" + dbinfo.getOutputCharset(), ToolLogger.INFO);
                logln("ConnectionURL: " + dbinfo.getConnectionURL(), ToolLogger.INFO);
                logln("", ToolLogger.INFO);
            }
            Properties props = new Properties();
            props.put("user", dbinfo.getUserName());
            props.put("password", dbinfo.getPassword());
            if(dbinfo.isDBAUser()) {
                props.put("internal_logon", "sysdba");
            }
            rcon = new RetainableConnection(dbinfo.getConnectionURL(), props);
            String checkQuery = dbinfo.getCheckQuery();
            if(checkQuery != null) {
	            rcon.setCheckQuerySQL(checkQuery);
	            rcon.setCheckQueryInterval(checkConnIdleTime);
	            rcon.setCheckQueryType(checkConnMethod);
	            rcon.resetCheckQuery();
            } else {
                DebugLogger.logln("DBMS doesn't support CheckQuery.", DebugLogger.WARN);
            	
            }
        } catch(Exception e) {
            logln(e.getMessage().trim(), ToolLogger.ERROR);
            logln("\nLogon denied.", ToolLogger.ERROR);
            DebugLogger.logln("Failed to create a connection : " + e.getMessage(), DebugLogger.ERROR);
        }

        if(rcon != null) {
            try {
                if(first) {
                    DatabaseMetaData meta = rcon.getMetaData();
                    SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d HH:mm:ss yyyy");
                    logln("");
                    logln(PRODUCT + ": Release " + VERSION + " - Production on " + sdf.format(new java.util.Date()), ToolLogger.INFO);
                    logln("Copyright (c) 2008-2010, Samsung SDS Corporation, Programed by Young-Dal,Kwon.\n", ToolLogger.INFO);
                    logln("Connected to:\n" + meta.getDatabaseProductVersion() + "\n", ToolLogger.INFO);
                    logln("JDBC Driver: " + meta.getDriverName() + " " + meta.getDriverVersion(), ToolLogger.INFO);
                    logln("Connection(" + rcon + ")[" + dbinfo.getUserName() + "] : " + (System.currentTimeMillis() - start) + " ms", ToolLogger.INFO);
                    logln("", ToolLogger.INFO);
                } else {
                    logln("Connected.", ToolLogger.RESULT);
                    logln("Connection(" + rcon + ")[" + dbinfo.getUserName() + "] : " + (System.currentTimeMillis() - start) + " ms", ToolLogger.INFO);
                }
                rcon.setAutoCommit(false);
            } catch(SQLException se) {
                logln(se.getMessage().trim(), ToolLogger.ERROR);
                DebugLogger.logln("Failed to use the connection : " + se.getMessage(), DebugLogger.ERROR);                
            }
        } else {
            logln("Warning: You are no longer connected to DB", ToolLogger.ERROR);
            DebugLogger.logln("No longer connected to DB.", DebugLogger.WARN);
            throw new Exception("Cannot connect to DB.");
        }

    }


    private void reconnect() throws Exception {
        DebugLogger.logln("Reconnecting to DB.", DebugLogger.WARN);
        String isCont = "";
        boolean first = true;
        while(!isCont.equals("y") && !isCont.equals("n")) {
            if(!first) {
                logln("invalid input. try again...", ToolLogger.ERROR);
            }
            isCont = Util.readLine("Do you want to connect with previous connection parameter(Y/N)? ", ToolLogger.INFO);
            isCont = (isCont==null)?"":isCont.trim().toLowerCase();
            first = false;
        }
        if(isCont.equals("y")) {
            closeConnection();
        	createConnection(true);
        }
    }


    private String readSQL(String prompt) {

        StringBuffer buffer = new StringBuffer();
        boolean start = true;
        boolean end = false;
        boolean tm = !(prompt.startsWith("SQL"));
        boolean gotFirstLine = false;
        boolean cont = true;
        String prom = "";
        String tmstr = "";
        int cnt = 2;
        int digit = 0;
        String line_org = "";
        String line_tmp = "";


        while(!end) {
            if(start) {
                prom = prompt;
                start = false;
            } else {
                digit = String.valueOf(cnt).length();
                tmstr = (tm)?(Util.getCurrentTime()+" "):"";
                prom = tmstr + Util.getColumn(null, 3-digit, " ", "", true) + cnt++ + "  ";
            }
            line_tmp = Util.readLine(prom, ToolLogger.INFO);
            line_org = line_tmp;
            line_tmp = line_tmp.trim();

            if(!gotFirstLine) {
                gotFirstLine = true;
                if(line_tmp.endsWith(";")) {
                    line_tmp = line_tmp.substring(0,line_tmp.length()-1);
                    buffer.append(line_tmp);
                    cont = false;
                    end = true;
                }
                cmd = line_tmp;

                String action = getAction(cmd);

                if(action.equals("query") == false) {
                    return action;
                }
            }

            if(cont) {
                if(line_tmp.length() < 1) {
                    return "blank";
                } else if(line_tmp.endsWith(";")) {
                    line_tmp = line_tmp.substring(0,line_tmp.length()-1);
                    buffer.append(line_tmp);
                    end = true;
                } else {
                    buffer.append(line_org);
                }
            }
        }

        cmd = buffer.toString();

        return "query";
    }


    private String getAction(String command) {

        if(command.trim().length() < 1) {
            return "blank";
        }

        StringTokenizer st = new StringTokenizer(command.trim(), " ");
        String first = (st.countTokens()>0)?st.nextToken():"";

        if(first.equalsIgnoreCase("help")) {
            return "help";
        } else if(first.equalsIgnoreCase("exit") || Util.isIncludeEquals(first.toLowerCase(), "q", "uit") || Util.isIncludeEquals(first.toLowerCase(), "by", "e")) {
            return "exit";
        } else if(first.equalsIgnoreCase("ls")) {
            return "table";
        } else if(first.equals("/*")) {
            return "comment";
        } else if(first.startsWith("@")) {
            return "script";
        } else if(first.startsWith("!")) {
            return "external";
        } else if(first.startsWith("--")) {
            return "no";
        } else if(command.equals("/")) {
            return "previous";
        } else if(command.equalsIgnoreCase("info")) {
            return "info";
        } else if(Util.isIncludeEquals(command.toLowerCase(), "ver", "sion")) {
            return "version";
        } else if(Util.isIncludeEquals(command.toLowerCase(), "ed", "it")) {
            return "edit";
        } else if(Util.isIncludeEquals(command.toLowerCase(), "cl", "ear") || command.equalsIgnoreCase("cls")) {
            return "clear";
        } else if(Util.isIncludeEquals(command.toLowerCase(), "l", "ist")) {
            return "showquery";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "spo", "ol")) {
            return "spool";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "desc", "ribe")) {
            return "describe";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "conn", "ect")) {
            return "connect";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "def", "ine") || Util.isIncludeEquals(first.toLowerCase(), "undef", "ine")) {
            return "define";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "set", null)) {
            return "setting";
        } else if(Util.isIncludeEquals(first.toLowerCase(), "col", "umn")) {
            return "column";
        } else if( first.equalsIgnoreCase("select") || first.equalsIgnoreCase("insert")   || first.equalsIgnoreCase("update")    || first.equalsIgnoreCase("delete")   ||
                   first.equalsIgnoreCase("create") || first.equalsIgnoreCase("alter")    || first.equalsIgnoreCase("drop")      || first.equalsIgnoreCase("truncate") ||
                   first.equalsIgnoreCase("commit") || first.equalsIgnoreCase("rollback") || first.equalsIgnoreCase("savepoint") || (dbinfo.getType()==DBInfo.MYSQL && first.equalsIgnoreCase("show")) ) {
            return "query";
        } else {
            return "unknown";
        }

    }


    private void readDOC(String prompt) {
        String line = "";
        String tm = "";
        int idx = -1;

        do {
            tm = (prompt.startsWith("DOC"))?"":(Util.getCurrentTime()+" ");
            line = Util.readLine(tm + "DOC> ", ToolLogger.INFO);
            idx = line.indexOf("*/");
        } while(idx < 0);
    }


    private List<String> readScript(File f) {
        List<String> l = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;

        try {
            fr = new FileReader(f);
            br = new BufferedReader(fr);
            StringTokenizer st = null;
            StringBuffer buffer = new StringBuffer();
            String first = null;
            String s = null;
            boolean readingQuery = false;
            boolean readingDoc = false;

            while((s = br.readLine()) != null) {
                if(s.trim().length() > 0) {
                    if(readingQuery) {
                        buffer.append("\n" + s);
                        if(s.endsWith(";")) {
                            l.add(buffer.toString().substring(0,buffer.toString().length()-1));
                            buffer = new StringBuffer();
                            readingQuery = false;
                        }
                    } else if(readingDoc) {
                        logln("DOC> " + s);
                        if(s.endsWith("*/")) {
                            readingDoc = false;
                        }
                    } else {
                        st = new StringTokenizer(s, " ");
                        first = (st.countTokens()>0)?st.nextToken():"";
                        if( first.equalsIgnoreCase("select") || first.equalsIgnoreCase("insert")   || first.equalsIgnoreCase("update")    || first.equalsIgnoreCase("delete")   ||
                            first.equalsIgnoreCase("create") || first.equalsIgnoreCase("alter")    || first.equalsIgnoreCase("drop")      || first.equalsIgnoreCase("truncate") ||
                            first.equalsIgnoreCase("commit") || first.equalsIgnoreCase("rollback") || first.equalsIgnoreCase("savepoint") || (dbinfo.getType()==DBInfo.MYSQL && first.equalsIgnoreCase("show")) ) {
                            if(s.endsWith(";")) {
                                l.add(s.substring(0,s.length()-1));
                            } else {
                                buffer.append(s);
                                readingQuery = true;
                            }
                        } else if(first.equalsIgnoreCase("/*")) {
                            if(!s.endsWith("*/")) {
                                logln("DOC> " + s);
                                readingDoc = true;
                            }
                        } else {
                            if(s.endsWith(";")) {
                                s = s.substring(0,s.length()-1);
                            }
                            l.add(s);
                            readingQuery = false;
                        }
                    }
                }
            }
        } catch(IOException ioe) {
        } finally {
            if(br != null)
                try { br.close(); } catch(Exception e) {}
        }

        return l;
    }


    private void printResultSet(ResultSet rs) throws SQLException {

        int rowCount = 0;
        try {
            rs.last();
            rowCount = rs.getRow();
            rs.beforeFirst();
        } catch(SQLException se) {
            logln(se.getMessage().trim(), ToolLogger.ERROR);
        }

        if(ToolLogger.getSilent() == false && checkResultSet && rowCount > checkSize) {
            String isCont = "";
            boolean first = true;
            while(!isCont.equals("y") && !isCont.equals("n")) {
                if(!first) {
                    logln("invalid input. try again...", ToolLogger.ERROR);
                }
                isCont = Util.readLine("ResultSet is " + rowCount + " rows. Continue(Y/N)? ", ToolLogger.INFO);
                isCont = (isCont==null)?"":isCont.trim().toLowerCase();
                first = false;
            }
            if(isCont.equals("n")) {
                logln("query is cancled.", ToolLogger.INFO);
                return;
            }
        }

        ResultSetMetaData meta = rs.getMetaData();
        int colCnt = meta.getColumnCount();
        int[][] colInfo = new int[colCnt][2];
        int[] fixedSize = new int[colCnt];
        int[] maxSize = new int[colCnt];
        List<List<String>> dataList = new ArrayList<List<String>>();
        List<Integer> colSize = new ArrayList<Integer>();
        String[][] colName = new String[colCnt][2];
        String[] colType = new String[colCnt];
        String[] colForm = new String[colCnt];
        String colData;
        int colTotal = 0;
        int colInLine = 0;
        int lineCnt = 1;
        int len = 0;
        long end = 0L;

        for(int i = 1; i <= colCnt; i++) {
            colName[i-1][0] = meta.getColumnName(i);
            colName[i-1][1] = (getColumnInfo(colName[i-1][0],SQLMinus.HEADING)==null)?meta.getColumnName(i):getColumnInfo(colName[i-1][0],SQLMinus.HEADING);
            colInfo[i-1][0] = meta.getColumnDisplaySize(i);
            colInfo[i-1][1] = meta.getColumnType(i);
            colType[i-1] = meta.getColumnTypeName(i);
            colForm[i-1] = (getColumnInfo(colName[i-1][0],SQLMinus.FORMAT)==null)?"":getColumnInfo(colName[i-1][0],SQLMinus.FORMAT);
            fixedSize[i-1] = getFormatSize(colForm[i-1].trim(), colType[i-1]);
        }

        while (rs.next()) {
            List<String> rowList = new ArrayList<String>();
            for(int i = 1; i <= colCnt; i++) {
                rowList.add(getColumnValue(i, colInfo[i-1][1], rs));
            }
            dataList.add(rowList);
        }

        end = System.currentTimeMillis() - start;

        for(int i = 0; i < colCnt; i++) {
            for(int j = 0; j < dataList.size(); j++) {
                colData = (dataList.get(j)).get(i);
                if(colType[i].equals("NUMBER") && fixedSize[i] != 0) {
                    len = getNumberTypeData(colData, colForm[i]).length();
                } else {
                    len = colData.getBytes().length;
                }
                maxSize[i] = (len > maxSize[i])?len:maxSize[i];
            }
            colInfo[i][0] = (maxSize[i]>=colInfo[i][0])?(maxSize[i]+2):colInfo[i][0];
            colInfo[i][0] = (meta.getColumnName(i+1).getBytes().length>=colInfo[i][0])?(meta.getColumnName(i+1).getBytes().length):colInfo[i][0];
        }

        for(int i = 0; i < colCnt; i++) {
            if(meta.getColumnTypeName(i+1).equals("NUMBER")) {
                if( (fixedSize[i]==0 && lineSize<maxSize[i]) || (fixedSize[i]!=0 && lineSize<fixedSize[i]) ) {
                    logln("data item " + i+1 + " (\"" + meta.getColumnName(i+1) + "\") will not fit on line", ToolLogger.ERROR);
                    return;
                }
            }
            int size = (maxSize[i]>colName[i][1].getBytes().length)?maxSize[i]:colName[i][1].getBytes().length;
            size = (size>colInfo[i][0])?size:colInfo[i][0];
            size = (fixedSize[i]==0)?size:fixedSize[i];
            if(colTotal+size > lineSize) {
                if(colInLine==0) {
                    size = lineSize;
                    colSize.add(new Integer(size));
                    if(i < colCnt-1) {
                        colSize.add(new Integer(-1));
                        lineCnt++;
                    }
                } else {
                    int colTotal2 = 0;
                    int freeCol = 0;
                    int size2 = 0;
                    int realIdx = 0;
                    for(int j = i+(lineCnt-1)-colInLine; j <= i+(lineCnt-1); j++) {
                        realIdx = (Util.getRealIndex(colSize, j)==-1)?(Util.getRealIndex(colSize, j-1)+1):Util.getRealIndex(colSize, j);
                        if(fixedSize[realIdx] == 0) {
                            size2 = (maxSize[realIdx]>colName[realIdx][1].getBytes().length)?maxSize[realIdx]:colName[realIdx][1].getBytes().length;
                            freeCol++;
                        } else {
                            size2 = fixedSize[realIdx];
                        }
                        colTotal2 += size2 + ((j==i)?0:1);
                    }
                    if(colTotal2 > lineSize) {
                        size = (size>lineSize)?lineSize:size;
                        colSize.add(new Integer(-1));
                        colSize.add(new Integer(size));
                        colTotal = size+1;
                        colInLine = 1;
                        lineCnt++;
                    } else {
                        int totRest = lineSize - colTotal2;
                        int quot = (int)(totRest / freeCol);
                        int enoughCol = 0;
                        colTotal = 0;
                        List<Integer> colSize_temp = new ArrayList<Integer>();
                        for(int j = i-colInLine; j <= i; j++) {
                            if(fixedSize[j] == 0) {
                                int max = (maxSize[j]>colName[j][1].getBytes().length)?maxSize[j]:colName[j][1].getBytes().length;
                                if(colInfo[j][0]-max < quot) {
                                    size = colInfo[j][0];
                                } else {
                                    size = max + ((quot<1)?0:quot);
                                    enoughCol++;
                                }
                                totRest -= size - max;
                            } else {
                                size = fixedSize[j];
                            }
                            colSize_temp.add(new Integer(size));
                        }
                        int rest = totRest / enoughCol;
                        for(int j = i-colInLine, k = 0; j <= i; j++, k++) {
                            size = (colSize_temp.get(k)).intValue();
                            if((fixedSize[j] == 0 && size != 0) || fixedSize[j] != 0) {
                                if(size+rest <= colInfo[j][0]) {
                                    size += rest;
                                }
                                if(Util.getColCntInList(colSize) <= j) {
                                    colSize.add(new Integer(size));
                                } else {
                                    colSize.set(Util.getListIndex(colSize,j), new Integer(size));
                                }
                            }
                            colTotal += ((size==0)?fixedSize[j]:size) + ((j==i)?0:1);
                        }
                        colInLine++;
                    }
                }
            } else {
                colSize.add(new Integer(size));
                colTotal += size+1;
                colInLine++;
            }
        }

        logln("");

        //logln(Util.getColSizeString(colSize), Logger.INFO);

        int size = 0;
        int lst_idx = 0;
        String newCol = "";
        for(int i = 0; i < dataList.size(); i++) {
            List<String> lst = dataList.get(i);
            for(int j = 0; j < lst.size(); j++) {
                do {
                    size = (colSize.get(lst_idx++)).intValue();
                } while(size == -1);
                newCol = Util.makePrintFormatString(lst.get(j), size);
                lst.set(j, newCol);
            }
            dataList.set(i, lst);
            lst_idx = 0;
        }

        printAllData(colName, colType, colForm, dataList, colSize);

        logln(((dataList.size()==0)?"no":("\n"+String.valueOf(dataList.size()))) + " rows selected.");
        if(showTiming) {
            logln("Query execute Finish : " + end + " ms");
        }
    }


    private void printResult(int rows, String query) throws Exception {
        String temp = query.trim().replaceAll("\n"," ").replaceAll("\r", "").replaceAll("\t", " ").toLowerCase();
        StringTokenizer st = new StringTokenizer(temp, " ");
        String first = (st.hasMoreTokens())?st.nextToken():"";
        String second = (st.hasMoreTokens())?st.nextToken():"";;
        String third = (st.hasMoreTokens())?st.nextToken():"";;
        String fourth = (st.hasMoreTokens())?st.nextToken():"";;

        logln("");
        if(first.equals("insert")) {
            logln(rows + " row created.");
        } else if(first.equals("update") || first.equals("delete")) {
            logln(rows + " row " + first + "d.");
        } else if(first.equals("create") || first.equals("truncate")) {
            if(second.equals("or") && third.equals("replace") && fourth.length() > 2) {
                logln(fourth.substring(0,1).toUpperCase() + fourth.substring(1) + " " + first + "d.");
            } else {
                logln(second.substring(0,1).toUpperCase() + second.substring(1) + " " + first + "d.");
            }
        } else if(first.equals("alter")) {
            logln(second.substring(0,1).toUpperCase() + second.substring(1) + " " + first + "ed.");
        } else if(first.equals("drop")) {
            logln(second.substring(0,1).toUpperCase() + second.substring(1) + " " + first + "ped.");
        }
    }


    private void printDescribe(ResultSetMetaData meta) throws Exception {
        int colCnt = meta.getColumnCount();
        String[] title = {"Name", "Null?", "Type"};
        int[] colSize = new int[3];
        int[] minColSize = {17, 8, 12};
        int[] maxSize = {0, 8, 12};
        String[] name = new String[colCnt];
        String[] type = new String[colCnt];
        int precision;
        int scale;
        int quot;
        int sum = 0;
        String digit;
        boolean longType;

        for(int i = 1; i <= colCnt; i++) {
            String colType = meta.getColumnTypeName(i);
            longType = (colType.equals("LONG") || colType.equals("BLOB") || colType.equals("CLOB")) ? true : false;
            if(!longType) {
                precision = meta.getPrecision(i);
                scale = meta.getScale(i);
            } else {
                precision = scale = 0;
            }
            digit = "";
            if(precision > 0 && !longType) {
                digit = "(" + String.valueOf(precision) + ((scale<1)?"":","+String.valueOf(scale)) + ")";
            }
            name[i-1] = meta.getColumnName(i);
            type[i-1] = meta.getColumnTypeName(i) + digit;
            maxSize[0] = (name[i-1].getBytes().length>maxSize[0])?name[i-1].getBytes().length:maxSize[0];
            maxSize[2] = (type[i-1].getBytes().length>maxSize[2])?type[i-1].getBytes().length:maxSize[2];
        }

        for(int i = 0; i < 3; i++) {
            if(i == 1) {
                colSize[i] = 8;
            } else {
                colSize[i] = (maxSize[i]>minColSize[i])?maxSize[i]:minColSize[i];
            }
            sum += 1+colSize[i];
        }
        if(lineSize > sum) {
            quot = (lineSize-sum) / 3;
            colSize[0] += quot*2;
            colSize[2] += quot;
        }

        log("\n ");

        for(int i = 0; i < title.length; i++) {
            Util.printColumn(title[i], colSize[i], " ", " ", true);
        }

        log("\n ");

        for(int i = 0; i < title.length; i++) {
            Util.printLine(colSize[i], 1);
        }

        log("\n");

        for(int i = 1; i <= colCnt; i++) {
            log(" ");
            Util.printColumn(meta.getColumnName(i), colSize[0], " ", " ", true);

            Util.printColumn(((meta.isNullable(i)==0)?"NOT NULL":""), colSize[1], " ", " ", true);

            String colType = meta.getColumnTypeName(i);
            longType = (colType.equals("LONG") || colType.equals("BLOB") || colType.equals("CLOB")) ? true : false;
            if(!longType) {
                precision = meta.getPrecision(i);
                scale = meta.getScale(i);
            } else {
                precision = scale = 0;
            }
            digit = "";
            if(precision > 0 && !longType) {
                digit = "(" + String.valueOf(precision) + ((scale<1)?"":","+String.valueOf(scale)) + ")";
            }

            Util.printColumn((meta.getColumnTypeName(i)+digit), colSize[2], " ", "\n", true);
        }

        if(showTiming) {
            logln("\nQuery execute Finish : " + (System.currentTimeMillis() - start) + " ms");
        }
    }


    private String getColumnValue(int index, int type, ResultSet rs) throws SQLException {
        String data = "";

        Object obj = rs.getObject(index);
        if(obj == null) {
            return data;
        }

        switch(type) {
            case Types.DATE:
                DateFormat df = new SimpleDateFormat("dd-MMM-yy", new Locale("en", "US"));
                if(rs.getDate(index) != null) {
                    data = df.format(rs.getDate(index)).toUpperCase();
                }
                break;
            case Types.CLOB:
                data = ((Clob)obj).getSubString(1, 80);
                break;
            case Types.BIT:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.TIME:
            case Types.TIMESTAMP:
            default:
                data = obj.toString();
        }

        try {
            if(!dbinfo.getOutputCharset().equals("")) {
                data = new String(data.getBytes("8859_1"), dbinfo.getOutputCharset());
            }
        } catch(UnsupportedEncodingException e) {
            logln("Cannot get data('" + data + "') with charset('" + dbinfo.getOutputCharset() + "')", ToolLogger.ERROR);
            logln("Please connect with valid charset.", ToolLogger.ERROR);
            System.exit(-1);
        }
        data = data.replaceAll("\n"," ").replaceAll("\r", "").replaceAll("\t", " ");
        return data;
    }


    public boolean isNumberFormat(String str) {
        boolean check = true;
        for(int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if(c!='0' && c!='9' && c!='$' && c!=',' && c!='.') {
                check = false;
                break;
            }
        }
        if(Util.getCharHowManyTimes(str, '$') > 1 || Util.getCharHowManyTimes(str, '.') > 1) {
            check = false;
        }
        if(str.indexOf('.') > -1 && str.indexOf(',') > -1 && str.indexOf('.') < str.lastIndexOf(',')) {
            check = false;
        }
        return check;
    }


    public void setColumnInfo(String colname, String type, String value) {
        Properties p = (Properties)setCols.get(colname);
        if(p == null) {
            p = new Properties();
            p.setProperty(SQLMinus.APPLY, "on");
            p.setProperty(type, value);
            setCols.put(colname, p);
        } else {
            p.setProperty(type, value);
            setCols.put(colname, p);
        }
    }


    public String getColumnInfo(String colname, String type) {
        Properties p = (Properties)setCols.get(colname.toUpperCase());
        if(p == null) {
            return null;
        } else {
            if(p.getProperty(SQLMinus.APPLY).equals("on")) {
                return p.getProperty(type);
            } else {
                return null;
            }
        }
    }


    public void printColumnSetting(String colname) {
        Properties p = (Properties)setCols.get(colname.toUpperCase());
        if(p == null) {
            logln("COLUMN '" + colname + "' not defined", ToolLogger.ERROR);
        } else {
            String apply = p.getProperty(SQLMinus.APPLY);
            String heading = p.getProperty(SQLMinus.HEADING);
            String format = p.getProperty(SQLMinus.FORMAT);
            String wrap = p.getProperty(SQLMinus.WRAPPED);

            logln("COLUMN\t" + colname + " " + apply.toUpperCase());
            if(heading != null) {
                logln("HEADING\t'" + heading + "'");
            }
            if(format != null) {
                logln("FORMAT\t" + format);
            }
            if(wrap != null) {
                logln(wrap);
            }
        }
    }


    public void printAllColumnsSetting() {
        String colname;
        Enumeration<String> keys = setCols.keys();
        int cnt = 0;
        while(keys.hasMoreElements()) {
            if(cnt != 0) {
                logln("");
            }
            colname = keys.nextElement();
            printColumnSetting(colname);
            cnt++;
        }
        if(cnt == 0) {
            logln("no column info", ToolLogger.RESULT);
        }
    }


    public void printQuery(String query) {
        StringTokenizer st = new StringTokenizer(query, "\n");
        String line = "";
        List<String> l = new ArrayList<String>();
        int digit = 0;

        while(st.hasMoreTokens()) {
            line += st.nextToken();
            l.add(line);
            line = "";
        }

        if(query.endsWith("\n")) {
            l.add("");
        }

        for(int i = 1; i <= l.size(); i++) {
            line = l.get(i-1);
            digit = String.valueOf(i).length();
            logln(Util.getColumn(null, 3-digit, " ", "", true) + String.valueOf(i) + ((i==l.size())?"* ":"  ") + line);
        }
    }


    public void printAllData(String[][] colName, String[] colType, String[] colForm, List<List<String>> dataList, List<Integer> colSize) {
        int size = 0;
        int maxLine = 0;
        int realIdx = 0;
        int newLineIdx = 0;
        int lineCol = 0;
        int colLine = 0;
        int totLineCnt = 0;
        int sumMaxLine = 0;
        int colCnt = Util.getColCntInList(colSize);
        boolean isNum = false;
        boolean newLine = false;
        String colData = "";
        String lineData = "";
        String endStr = "";
        String[] wrap = new String[colCnt];
        List<String> rowData;

        for(int j = 0; j < colCnt; j++) {
            wrap[j] = getColumnInfo(colName[j][0], SQLMinus.WRAPPED);
            wrap[j] = (wrap[j]==null)?"wrap":wrap[j];
        }

        for(int i = 0; i < dataList.size(); i++) {
            sumMaxLine = 0;
            rowData = dataList.get(i);
            for(int j = 0; j < colSize.size();) {
                newLineIdx = Util.indexOfList(colSize, -1, j);
                if(newLineIdx > -1) {
                    lineCol = newLineIdx - j;
                } else {
                    lineCol = colSize.size() - j;
                }

                maxLine = 0;
                for(int k = 0; k < lineCol; k++) {
                    realIdx = Util.getRealIndex(colSize, j+k);
                    colData = (String)rowData.get(realIdx);
                    colLine = (wrap[realIdx].equals("truncate"))?1:Util.getLineCount(colData);
                    maxLine = (maxLine<colLine)?colLine:maxLine;
                }
                sumMaxLine += maxLine;

                for(int k = 0; k < maxLine; k++) {
                    if(pageSize > 0 && (totLineCnt%pageSize) == 0) {
                        if(totLineCnt != 0) {
                            logln("");
                        }
                        printTitle(colSize, colName, colType);
                        if(newLine) {
                            logln("");
                            totLineCnt++;
                            newLine = false;
                        }
                    }

                    for(int l = 0; l < lineCol; l++) {
                        realIdx = Util.getRealIndex(colSize, j+l);
                        colData = (String)rowData.get(realIdx);
                        if(wrap[realIdx].equals("truncate") && k > 0) {
                            lineData = "";
                        } else {
                            lineData = Util.getLineData(colData, k);
                        }
                        size = ((Integer)colSize.get(j+l)).intValue();
                        endStr = (l<lineCol-1)?" ":"\n";
                        isNum = (colType[realIdx].equals("NUMBER"))?true:false;
                        if(isNum) {
                            if(k == 0) {
                                if(!colForm[realIdx].equals("")) {
                                    lineData = getNumberTypeData(lineData, colForm[realIdx]);
                                }
                            } else {
                                lineData = "";
                            }
                        }
                        Util.printColumn(lineData, size, " ", endStr, !isNum);
                    }

                    totLineCnt++;
                }

                if(newLineIdx > -1) {
                    j = newLineIdx + 1;
                } else {
                    j = colSize.size();
                }
            }
            if(sumMaxLine > 1) {
                if(pageSize > 0 && (totLineCnt%pageSize) == 0) {
                    newLine = true;
                } else {
                    logln("");
                    totLineCnt++;
                }
            }
        }
    }


    public void printTitle(List<Integer> colSize, String[][] colName, String[] colType) {
        if(showHead) {
            int t_size = -1;
            int t_idx = 0;
            int t_firstColIdx = 0;
            int t_col = 0;
            boolean t_firstInLine = true;
            for(int i = 0; i < colSize.size(); i++) {
                t_size = (colSize.get(i)).intValue();
                if(t_size != -1) {
                    log((t_firstInLine)?"":" ");
                    if(colType[t_idx].equals("NUMBER")) {
                        Util.printData(colName[t_idx++][1], t_size, 0, 0, false);
                    } else {
                        Util.printData(colName[t_idx++][1], t_size, 0, 0, true);
                    }
                    t_firstInLine = false;
                    t_col++;
                } else {
                    if(t_col != 0) {
                        log("\n");
                    }
                    for(int j = t_firstColIdx; j < t_col; j++) {
                        Util.printLine(getColumnSize(colSize, j), ((j<t_col-1)?1:0));
                    }
                    log("\n");
                    t_firstColIdx = t_col;
                    t_firstInLine = true;
                }
            }
            logln("");
            for(int i = t_firstColIdx; i < t_col; i++) {
                Util.printLine(getColumnSize(colSize, i), ((i<t_col-1)?1:0));
            }
            log("\n");
        }
    }


    public int getColumnSize(List<Integer> colSize, int i) {
        int idx = Util.getListIndex(colSize, i);
        return (colSize.get(idx)).intValue();
    }


    public int sizeIndexOf(List<Integer> lst, int beginIdx, int size) {
        int data = 0;
        int i = 0;
        for(i = beginIdx; i < lst.size(); i++) {
            data = (lst.get(i)).intValue();
            if(data == size) {
                return i;
            }
        }
        return i;
    }


    public boolean checkFormat(String fm) {
        if(fm.length() < 1)
            return false;
        if(isNumberFormat(fm)) {
            if(fm.length() > 99) {
                return false;
            } else {
                return true;
            }
        }
        if(fm.substring(0,1).equalsIgnoreCase("a") && Util.isNumber(fm.substring(1))) {
            if(Integer.parseInt(fm.substring(1)) > 60000) {
                return false;
            } else {
                return true;
            }
        }
        return false;
    }


    public int getFormatSize(String fm, String type) {
        if(fm == null || fm.equals("")) {
            return 0;
        }
        if(type.equals("NUMBER")) {
            if(fm.substring(0,1).equalsIgnoreCase("a")) {
                return 0;
            } else {
                return fm.length();
            }
        } else {
            if(fm.substring(0,1).equalsIgnoreCase("a")) {
                return Integer.parseInt(fm.substring(1));
            } else {
                return 0;
            }
        }
    }


    public String getNumberTypeData(String data, String fm) {
        StringBuffer result = new StringBuffer("");
        String form = fm;
        String data_left = "";
        String data_right = "";
        String form_left = "";
        String form_right = "";
        String temp = "";
        int idx = 0;

        if(fm.indexOf('$') > -1) {
            result.append("$");
            form = Util.getStrExcludeChar(fm, '$');
        }

        if(data.indexOf('.') > -1) {
            data_left = data.substring(0, data.indexOf('.'));
            data_right = data.substring(data.indexOf('.')+1, data.length());
        } else {
            data_left = data;
        }
        if(form.indexOf('.') > -1) {
            form_left = form.substring(0, form.indexOf('.'));
            form_right = form.substring(form.indexOf('.')+1, form.length());
        } else {
            form_left = form;
        }

        temp = Util.getStrExcludeChar(form_left, ',');
        if(data_left.length() <= temp.length()) {
            idx = temp.length() - data_left.length();
            for(int i = 0; i < form_left.indexOf('0'); i++) {
                form_left = Util.replaceChar(form_left, i, ",", ' ');
            }
            for(int i = form_left.indexOf('0'); i < idx; i++) {
                if(form_left.indexOf('0') < 0) {
                    form_left = Util.replaceChar(form_left, i, ",", ' ');
                } else {
                    form_left = Util.replaceChar(form_left, i, ",", '0');
                }
            }
            for(int i = idx; i < temp.length(); i++) {
                form_left = Util.replaceChar(form_left, i, ",", data_left.charAt(i-idx));
            }
        } else {
            return Util.getColumn(null, fm.length(), "#", "", true);
        }

        if(data_right.length() <= form_right.length()) {
            for(int i = 0; i < form_right.length(); i++) {
                form_right = Util.replaceChar(form_right, i, null, '0');
            }
            idx = form_right.length() - data_right.length();
            for(int i = 0; i < form_right.length()-idx; i++) {
                form_right = Util.replaceChar(form_right, i, null, data_right.charAt(i));
            }
        } else {
            for(int i = 0; i < form_right.length(); i++) {
                form_right = Util.replaceChar(form_right, i, null, data_right.charAt(i));
            }
        }

        result.append(form_left.trim());
        if(!form_right.trim().equals("")) {
            result.append("." + form_right.trim());
        }

        return result.toString();
    }


    public String editSQL(String buffer) {

        try {
            StringBuffer result = new StringBuffer("");
            String fname = ".sqlmedt.buf";
            String tmpdir = System.getProperty("java.io.tmpdir");
            String fs = System.getProperty("file.separator");
            String cmd = (fs.equals("/"))?"vi":"notepad";
            String abs_fname = tmpdir + fs + fname;
            File f = new File(abs_fname);

            FileWriter fw = new FileWriter(f);
            BufferedWriter bw = new BufferedWriter(fw);
            buffer = buffer.replaceAll("\r\n", "\n").replaceAll("\n", "\r\n");
            bw.write(buffer);
            bw.newLine();
            bw.write("/");
            bw.close();

            Process p = Runtime.getRuntime().exec(cmd + " " + abs_fname);
            p.waitFor();

            FileReader fr = new FileReader(f);
            BufferedReader br = new BufferedReader(fr);
            logln("Wrote file " + fname + "\r\n", ToolLogger.INFO);
            String s = null;
            int digit = 0;
            List<String> l = new ArrayList<String>();
            while((s = br.readLine()) != null) {
                l.add(s);
            }
            for(int line = 1; line <= l.size(); line++) {
                if(!(line == l.size() && (l.get(line-1)).equals("/"))) {
                    digit = String.valueOf(line).length();
                    log(Util.getColumn(null, 3-digit, " ", "", true) + line + "  " + l.get(line-1) + "\n", ToolLogger.INFO);
                    result.append(((line==1||line==l.size())?"":"\r\n") + l.get(line-1));
                }
            }
            br.close();

            if(f.exists()) {
                f.delete();
            }

            String r = result.toString();
            return (r.endsWith(";")?r.substring(0,r.length()-1):r);
        } catch(Exception e) {
            logln(e.getMessage().trim(), ToolLogger.ERROR);
            return null;
        }

    }


    private String replaceQueryVariable(String query) {
        String result = query;
        String patternStr = "&[a-zA-Z0-9]{1,}";
        String var = null;
        String value = null;
        String line_old = "";
        String line_new = "";
        int cnt = 0;
        int line_cnt = 1;
        int digit = 0;
        StringTokenizer st = new StringTokenizer(query, "\n");
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = null;

        while(st.hasMoreTokens()) {
            cnt = 0;
            line_old = st.nextToken();
            line_new = line_old;
            matcher = pattern.matcher(line_old);

            while(matcher.find()) {
                var = matcher.group();
                value = Util.getDataFromList(defines, var.substring(1).toUpperCase());
                digit = String.valueOf(line_cnt).length();

                if(value == null) {
                    value = Util.readLine("Enter value for " + var.substring(1) + ": ", ToolLogger.INFO).replaceAll("\r\n", "");
                }

                line_new = Util.replaceString(line_new, var, value);
                result = result.replaceAll(var, value);
                cnt++;
            }

            if(cnt > 0) {
                logln("old " + Util.getColumn(null, 3-digit, " ", "", true) + String.valueOf(line_cnt) + ": " + line_old, ToolLogger.INFO);
                logln("new " + Util.getColumn(null, 3-digit, " ", "", true) + String.valueOf(line_cnt) + ": " + line_new, ToolLogger.INFO);
            }

            line_cnt++;
        }

        return result;
    }


    public void runProcess(String[] command) {

        BufferedReader isb = null;
        BufferedReader esb = null;

        try {
            String results;
            Process p = Runtime.getRuntime().exec(command);
            isb = new BufferedReader(new InputStreamReader(p.getInputStream()));
            esb = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            while((results=isb.readLine()) != null) {
                logln(results, ToolLogger.INFO);
            }
            while((results=esb.readLine()) != null) {
                logln(results, ToolLogger.INFO);
            }
            logln("", ToolLogger.INFO);
        } catch(Exception e) {
            logln(e.getMessage().trim(), ToolLogger.ERROR);
        } finally {
            if(isb != null)
                try { isb.close(); } catch(Exception e) {}
            if(esb != null)
                try { esb.close(); } catch(Exception e) {}
        }

    }


    public void setSavePoint(String name, Savepoint sp) {
        Hashtable<String,Savepoint> ht = null;
        for(int i = 0; i < saves.size(); i++) {
            ht = saves.get(i);
            if(ht.containsKey(name)) {
                Hashtable<String,Savepoint> temp_ht = new Hashtable<String,Savepoint>();
                temp_ht.put(name, sp);
                saves.set(i, temp_ht);
                return;
            }
        }
        Hashtable<String,Savepoint> temp_ht = new Hashtable<String,Savepoint>();
        temp_ht.put(name, sp);
        saves.add(temp_ht);
        return;
    }


    public Savepoint getSavePoint(String name) {
        Hashtable<String,Savepoint> ht = null;
        for(int i = 0; i < saves.size(); i++) {
            ht = saves.get(i);
            if(ht.containsKey(name)) {
                Savepoint sp = ht.get(name);
                int j = i+1;
                while(j < saves.size()) {
                    saves.remove(j++);
                }
                return sp;
            }
        }
        return null;
    }


    public void clearSavePoint() {
        saves.clear();
    }


    public boolean doTransactionWork(String query) {
        try {
            if(query.toLowerCase().startsWith("commit")) {
                StringTokenizer st = new StringTokenizer(query.toLowerCase(), " ");
                if(st.countTokens() == 1) {
                    rcon.commit();
                    clearSavePoint();
                    logln("\nCommit complete.\n", ToolLogger.RESULT);
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    String temp2 = st.nextToken();
                    if(temp2.equals("work")) {
                        rcon.commit();
                        clearSavePoint();
                        logln("\nCommit complete.\n", ToolLogger.RESULT);
                    } else {
                        logln("a token other than WORK follows COMMIT", ToolLogger.ERROR);
                    }
                } else {
                    logln("a token other than WORK follows COMMIT", ToolLogger.ERROR);
                }
                return true;
            } else if(query.toLowerCase().startsWith("rollback")) {
                StringTokenizer st = new StringTokenizer(query.toLowerCase(), " ");
                if(st.countTokens() == 1) {
                    rcon.rollback();
                    clearSavePoint();
                    logln("\nRollback complete.\n", ToolLogger.RESULT);
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    String temp2 = st.nextToken();
                    if(temp2.equals("work")) {
                        rcon.rollback();
                        clearSavePoint();
                        logln("\nRollback complete.\n", ToolLogger.RESULT);
                    } else if(temp2.equals("to")) {
                        logln("savepoint name expected", ToolLogger.ERROR);
                    } else {
                        logln("invalid option to ROLLBACK WORK", ToolLogger.ERROR);
                    }
                } else if(st.countTokens() == 3) {
                    st.nextToken();
                    String temp2 = st.nextToken();
                    String sp_name = st.nextToken();
                    if(temp2.equals("to")) {
                        Savepoint sp = getSavePoint(sp_name);
                        if(sp == null) {
                            logln("savepoint '" + sp_name + "' never established", ToolLogger.ERROR);
                        } else {
                            rcon.rollback(sp);
                            logln("\nRollback complete.\n", ToolLogger.RESULT);
                        }
                    } else {
                        logln("invalid option to ROLLBACK WORK", ToolLogger.ERROR);
                    }
                } else if(st.countTokens() > 3) {
                    logln("SQL command not properly ended", ToolLogger.ERROR);
                }
                return true;
            } else if(query.toLowerCase().startsWith("savepoint")) {
                StringTokenizer st = new StringTokenizer(query.toLowerCase(), " ");
                if(st.countTokens() == 2) {
                    st.nextToken();
                    String sp_name = st.nextToken();
                    Savepoint sp = rcon.setSavepoint(sp_name);
                    setSavePoint(sp_name, sp);
                    logln("\nSavepoint created.\n", ToolLogger.RESULT);
                } else {
                    if(st.countTokens() == 1) {
                        logln("unimplemented feature", ToolLogger.ERROR);
                    } else if(st.countTokens() > 2) {
                        logln("SQL command not properly ended", ToolLogger.ERROR);
                    }
                }
                return true;
            }
        } catch(SQLException se) {
            se.getMessage();
            return true;
        } catch(AbstractMethodError ame) {
            logln("JDBC Driver version might be too old", ToolLogger.ERROR);
            return true;
        } catch(Exception e) {
            return true;
        }
        return false;
    }


    public void printProgramVersion() {
        logln("", ToolLogger.RESULT);
        logln(PRODUCT + " " + VERSION + "\n", ToolLogger.RESULT);
        logln("Samsung SDS, Co. Copyright(C) 2008-2009. All rights reserved.", ToolLogger.RESULT);
        logln("Programed by Young-Dal,Kwon.\n", ToolLogger.RESULT);
        System.exit(0);
    }


    public void printHelp(String topic) {
        if(topic == null) {
            logln("");
            logln(" HELP");
            logln(" ----");
            logln("");
            logln(" Accesses this command line help system. Enter HELP INDEX for a list of topics.");
            logln("");
            logln(" HELP [ALL|topic]");
            logln("");
            logln("");
        } else if(topic.equalsIgnoreCase("all")) {
            logln("");
            logln(" HELP");
            logln(" ----");
            logln("");
            logln(" Following is the list of available topic.");
            logln("");
            logln("   CL[EAR]|CLS");
            logln("   CONN[ECT]");
            logln("   COL[UMN]");
            logln("   DEF[INE]|UNDEF[INE]");
            logln("   DESC[RIBE]");
            logln("   ED[IT]");
            logln("   EXIT|Q[UIT]|BY[E]");
            logln("   L[IST]");
            logln("   LS");
            logln("   SET");
            logln("   SPO[OL]");
            logln("   VER[SION]");
            logln("   INFO");
            logln("   /");
            logln("   !");
            logln("   @");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "ver", "sion")) {
            logln("");
            logln(" VERSION");
            logln(" -------");
            logln("");
            logln(" Display version of program, DBMS and JDBC driver.");
            logln("");
            logln(" VER[SION]");
            logln("");
        } else if(topic.equalsIgnoreCase("info")) {
            logln("");
            logln(" INFO");
            logln(" ----");
            logln("");
            logln(" Display current information of connection, such as:");
            logln("     - DBMS Type");
            logln("     - Connection Info(IP, PORT, DBNAME, USER, PASSWORD, PARAMETERS, URL)");
            logln("     - Character set(IN, OUT)");
            logln("");
            logln(" INFO");
            logln("");
        } else if(topic.equalsIgnoreCase("set")) {
            logln("");
            logln(" SET");
            logln(" ---");
            logln("");
            logln(" Sets a system variable to alter the SQL*Minus environment settings");
            logln(" for your current session, for example:");
            logln("     -   display width for data");
            logln("     -   enabling or disabling printing of column headings");
            logln("     -   number of lines per page");
            logln("     -   number of seconds waiting for executing query");
            logln("     -   turn on checking rows of result");
            logln("     -   turn on checking connection by check query");
            logln("     -   number of seconds waiting for check query");
            logln("");
            logln(" SET system_variable value");
            logln("");
            logln(" where system_variable and value represent one of the following clauses:");
            logln("");
            logln("   PAGES[IZE] {24|n}");
            logln("   LIN[ESIZE] {80|n}");
            logln("   CHECKS[IZE] {1000|n}");
            logln("   TIMEO[UT] {60|n}");
            logln("   HEA[DING] {ON|OFF}");
            logln("   TIMI[NG] {OFF|ON}");
            logln("   TI[ME] {OFF|ON}");
            logln("   SCAN {ON|OFF}");
            logln("   CHECK {ON|OFF}");
            logln("   CHECKCONN {OFF|FIRST_CONNECT|PRE_REQUEST|IDLE_CHECK|ALL}");
            logln("   CHECKCONN_I[DLETIME] {5|n}");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "col", "umn")) {
            logln("");
            logln(" COLUMN");
            logln(" ------");
            logln("");
            logln(" Specifies display attributes for a given column, such as:");
            logln("     - column heading text");
            logln("     - column heading alignment");
            logln("     - data format");
            logln("     - column data wrapping");
            logln("");
            logln(" Also lists the current display attributes for a single column");
            logln(" or all columns.");
            logln("");
            logln(" COL[UMN] [{column | expr} [option ...] ]");
            logln("");
            logln(" where option represents one of the following clauses:");
            logln("     CLE[AR]");
            logln("     FOR[MAT] format");
            logln("     HEA[DING] text");
            logln("     ON|OFF");
            logln("     WRA[PPED] | TRU[NCATED]");
            logln("");
        } else if(topic.equalsIgnoreCase("ls")) {
            logln("");
            logln(" LS");
            logln(" --");
            logln("");
            logln(" Displays objects");
            logln("");
            logln(" This command is supported in following DBMS:");
            logln("     - Oracle");
            logln("     - Tibero");
            logln("");
            logln(" LS [object_type]");
            logln("");
            logln(" where object_type is");
            logln("   INDEX");
            logln("   PROCEDURE");
            logln("   SEQUENCE");
            logln("   SYNONYM");
            logln("   TABLE");
            logln("   TABLESPACE");
            logln("   TRIGGER");
            logln("   USER");
            logln("   VIEW");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "l", "ist")) {
            logln("");
            logln(" LIST");
            logln(" ----");
            logln("");
            logln(" Lists lines of the most recently executed SQL command in the SQL buffer.");
            logln("");
            logln(" L[IST]");
            logln("");
        } else if(topic.equals("/")) {
            logln("");
            logln(" / (execute)");
            logln(" -----------");
            logln("");
            logln(" Executes the most recently executed SQL command.");
            logln(" Use '/' at the command prompt in SQL*Minus command line.");
            logln("");
            logln(" /");
            logln("");
        } else if(topic.equals("!")) {
            logln("");
            logln(" ! (external)");
            logln(" ------------");
            logln("");
            logln(" Executes external(OS) command.");
            logln(" Use the command line with '!' at the command prompt in SQL*Minus command line.");
            logln("");
            logln(" ![command_line]");
            logln("");
        } else if(topic.equals("@")) {
            logln("");
            logln(" @ (script)");
            logln(" ----------");
            logln("");
            logln(" Executes the script file included SQL commands.");
            logln(" Use the name of file with '@' at the command prompt in SQL*Minus command line.");
            logln("");
            logln(" @[file_name[.ext]]");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "desc", "ribe")) {
            logln("");
            logln(" DESCRIBE");
            logln(" --------");
            logln("");
            logln(" Lists the column definitions for a table, view, or synonym.");
            logln("");
            logln(" DESC[RIBE] {[schema.]object}");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "cl", "ear") || topic.equals("cls")) {
            logln("");
            logln(" CLEAR");
            logln(" -----");
            logln("");
            logln(" Clear the current display screen.");
            logln("");
            logln(" {CL[EAR]|CLS}");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "conn", "ect")) {
            logln("");
            logln(" CONNECT");
            logln(" -------");
            logln("");
            logln(" Connects a given username to DBMS.");
            logln("");
            logln(" CONN[ECT] [logon]");
            logln("");
            logln(" where logon has the following syntax:");
            logln("     username[/password][@connect_identifier]");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "ed", "it")) {
            logln("");
            logln(" EDIT");
            logln(" ----");
            logln("");
            logln(" Invokes a host operating system text editor on the contents of");
            logln(" the specified file or on the contents of the SQL buffer.");
            logln("");
            logln(" ED[IT] [file_name[.ext]]");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "spo", "ol")) {
            logln("");
            logln(" SPOOL");
            logln(" -----");
            logln("");
            logln(" Stores query results in an operating system file.");
            logln("");
            logln(" SPO[OL] [file_name[.ext] | OFF] [APPEND]");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "def", "ine")) {
            logln("");
            logln(" DEFINE");
            logln(" ------");
            logln("");
            logln(" Specifies a user variable and assigns a CHAR value to it, or");
            logln(" lists the value and variable type of a single variable or all");
            logln(" variables.");
            logln("");
            logln(" DEF[INE] [variable] | [variable = text]");
            logln("");
        } else if(Util.isIncludeEquals(topic.toLowerCase(), "undef", "ine")) {
            logln("");
            logln(" UNDEFINE");
            logln(" --------");
            logln("");
            logln(" Deletes one or more user variables that you defined either");
            logln(" explicitly (with the DEFINE command), or implicitly (with a START");
            logln(" command argument).");
            logln("");
            logln(" UNDEF[INE] variable ...");
            logln("");
        } else if(topic.equalsIgnoreCase("exit") || Util.isIncludeEquals(topic.toLowerCase(), "q", "uit") || Util.isIncludeEquals(topic.toLowerCase(), "by", "e")) {
            logln("");
            logln(" " + topic.toUpperCase() + ((topic.equalsIgnoreCase("exit"))?"":" (Identical to EXIT)"));
            logln(" " + Util.getColumn("", topic.length(), "-", "", true));
            logln("");
            logln(" Commits or rolls back all pending changes, logs out of DBMS,");
            logln(" terminates SQL*Minus and returns control to the operating system.");
            logln(" All changes are commited as default when log-out.");
            logln("");
            logln(" {EXIT|Q[UIT]|BY[E]} [COMMIT|ROLLBACK]");
            logln("");
        } else {
            logln("No HELP available.");
        }
        return;
    }


    public void printMainHelp() {
        logln("");
        logln("Usage: SQLMinus [<option>] [<parameter> ...]");
        logln("");
        logln("option");
        logln("------");
        logln("  -h,/h,-help,/help,-?,/?");
        logln("     Displays this information");
        logln("");
        logln("  -v,/v,-version,/version");
        logln("     Displays version information");
        logln("");
        logln("  -s,/s,-silent,/silent");
        logln("     Uses silent mode");
        logln("");
        logln("  -d,/d,-debug,/debug");
        logln("     Writes debugging logs in sqlm_dbg.log");
        logln("");
        logln("parameter");
        logln("---------");
        logln("  [driver]");
        logln("    driver=JDBCConnectionURL");
        logln("      JDBC Driver Path to add to classpath");
        logln("      JDBC Driver depends on the type of DBMS");
        logln("      can input 2 or more files with semicolon(;) separator");
        logln("");
        logln("  [url]");
        logln("    url=JDBCConnectionURL");
        logln("      String to connect to database");
        logln("      ConnectionURL depends on the type of DBMS");
        logln("");
        logln("  [logon]");
        logln("    logon=username[/password]");
        logln("      username and password to connect to database");
        logln("");
        logln("  [encoding]");
        logln("    encoding=[InputCharset[/OutputCharset]]");
        logln("      Input Charset  : charset in java to encoding queries");
        logln("      Output Charset : charset in java to encoding result data");
        logln("      * blank for default encoding");
        logln("      * '-' for no encoding");
        logln("");
        logln("  [script]");
        logln("    script=filename[.ext]");
        logln("      filename to execute on start");
        logln("");
        logln("  ex)");
        logln("    SQLMinus logon=scott/tiger");
        logln("    SQLMinus url=jdbc:oracle:thin:@192.168.1.1:1521:ora encoding=-/-");
        logln("    SQLMinus -s url=jdbc:oracle:thin:@192.168.1.1:1521:ora logon=scott/tiger encoding= script=a.sql");
        logln("");
        logln("JDBC Driver");
        logln("-----------");
        logln("  Need the JDBC Driver that is offered from DBMS vendor.");
        logln("  JDBC Driver should be placed in current directory.");
        logln("");

        System.exit(0);
    }


    private void printErrorOnStart(String msg) {
        logln(msg, ToolLogger.ERROR);
        DebugLogger.logln("Error occurred while starting. "+msg, DebugLogger.ERROR);
        printMainHelp();
    }


    private void logln(Object msg) {
        ToolLogger.logln(msg, ToolLogger.RESULT);
    }


    private void log(Object msg) {
        ToolLogger.log(msg, ToolLogger.RESULT);
    }


    private void logln(Object msg, int level) {
        ToolLogger.logln(msg, level);
    }


    private void log(Object msg, int level) {
        ToolLogger.log(msg, level);
    }


    private boolean processAction(String action, String query, String tm, LinkedList<String> ll) throws Exception {

        if(action.equals("script")) {

            if(cmd.substring(1).trim().length() < 1) {
                logln("unable to open file \"\"", ToolLogger.ERROR);
                return false;
            }

            StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(1,cmd.length()-1).trim():cmd.substring(1), " ");
            boolean ret = true;
            for(int i = 0; i < st.countTokens(); i++) {
                String fname = Util.checkFileName(st.nextToken(), "sql");
                if(ll.contains(fname)) {
                    String pre = ll.getLast();
                    logln("Infinite loop has been detected in \"" + pre + "\"", ToolLogger.ERROR);
                    return false;
                } else {
                    ll.addLast(fname);
                }
                if(fname.length() > 0) {
                    File f = new File(fname);
                    if(f.exists()) {
                        List<String> l = readScript(f);
                        String subaction;
                        for(int j = 0; j < l.size(); j++) {
                            cmd = l.get(j);
                            subaction = getAction(cmd);
                            ret = processAction(subaction, cmd, tm, ll);
                            if(ret == false) {
                                return false;
                            }
                        }
                    } else {
                        logln("unable to open file \"" + fname + "\"", ToolLogger.ERROR);
                        return true;
                    }
                }
            }
            ll.removeLast();
            return true;

        } else {

            if(action.equals("exit")) {

                ToolLogger.setSpoolOff();

                loop = false;

                StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(0,cmd.length()-1).trim():cmd, " ");
                if(st.countTokens() == 2) {
                    st.nextToken();
                    String second = st.nextToken();
                    if(second.equalsIgnoreCase("commit")) {
                        commitOnExit = true;
                    } else if(second.equalsIgnoreCase("rollback")) {
                        commitOnExit = false;
                    } else {
                        logln("Usage: { EXIT | Q[UIT] | BY[E] }  [ COMMIT | ROLLBACK ]", ToolLogger.ERROR);
                    }
                } else if(st.countTokens() > 2) {
                    logln("Usage: { EXIT | Q[UIT] | BY[E] }  [ COMMIT | ROLLBACK ]", ToolLogger.ERROR);
                }
                closeConnection();
                return false;

            } else if(action.equals("help")) {

                StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(0,cmd.length()-1).trim():cmd, " ");
                if(st.countTokens() == 1) {
                    printHelp(null);
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    String second = st.nextToken();
                    printHelp(second);
                } else {
                    logln("No HELP available.", ToolLogger.ERROR);
                }
                return true;

            } else if(action.equals("version")) {

                logln(PRODUCT + ": Release " + VERSION, ToolLogger.INFO);
                if(rcon == null) {
                    logln("", ToolLogger.ERROR);
                    logln("Not connected to server.", ToolLogger.ERROR);
                    try {
                    	reconnect();
                    } catch(Exception e) {
                        return false;
                    }
                }
                DatabaseMetaData meta = rcon.getMetaData();
                logln("JDBC Driver: " + meta.getDriverName() + " " + meta.getDriverVersion(), ToolLogger.INFO);
                logln("Connected to:\n" + meta.getDatabaseProductVersion(), ToolLogger.INFO);
                logln("", ToolLogger.INFO);
                return true;

            } else if(action.equals("info")) {

                dbinfo.printInfo();
                return true;

            } else if(action.equals("table")) {

                StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(0,cmd.length()-1).trim():cmd, " ");
                if(st.countTokens() == 1) {
                    query = dbinfo.getLSQuery(null);
                    if(query.equals("")) {
                        logln("Not supported command.", ToolLogger.ERROR);
                        return true;
                    }
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    String second = st.nextToken();
                    query = dbinfo.getLSQuery(second);
                    if(query.equals("")) {
                        logln("Not supported command.", ToolLogger.ERROR);
                        return true;
                    }
                } else {
                    logln("Not supported command.", ToolLogger.ERROR);
                    return true;
                }

            } else if(action.equals("describe")) {

                StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(0,cmd.length()-1).trim():cmd, " ");
                if(st.countTokens() != 2) {
                    logln("Usage: DESC[RIBE] object", ToolLogger.ERROR);
                    return true;
                } else {
                    st.nextToken();
                    String second = st.nextToken();
                    query = dbinfo.getDESCQuery(second);
                }

            } else if(action.equals("spool")) {

                StringTokenizer st = new StringTokenizer(cmd.endsWith(";")?cmd.substring(0,cmd.length()-1).trim():cmd, " ");
                if(st.countTokens() == 1) {
                    if(ToolLogger.getSpoolName() == null) {
                        logln("not spooling currently", ToolLogger.ERROR);
                    } else {
                        logln("currently spooling to " + ToolLogger.getSpoolName(), ToolLogger.RESULT);
                    }
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    String second = st.nextToken();
                    if(second.equalsIgnoreCase("off")) {
                        if(ToolLogger.getSpoolName() == null) {
                            logln("not spooling currently", ToolLogger.ERROR);
                        } else {
                            ToolLogger.setSpoolOff();
                        }
                    } else {
                        ToolLogger.setSpoolName(Util.checkFileName(second, "lst"), false);
                    }
                } else if(st.countTokens() == 3) {
                    st.nextToken();
                    String second = st.nextToken();
                    String third = st.nextToken();
                    if(third.equalsIgnoreCase("append")) {
                        ToolLogger.setSpoolName(Util.checkFileName(second, "lst"), true);
                    } else {
                        logln("unknown command \"" + third + "\" - only append option is allowed.", ToolLogger.ERROR);
                    }
                }
                return true;

            } else if(action.equals("comment")) {

                readDOC(tm + "DOC> ");
                return true;

            } else if(action.equals("external")) {

                String full_cmd = cmd.endsWith(";")?cmd.substring(1,cmd.length()-1).trim():cmd.substring(1);
                StringTokenizer st = new StringTokenizer(full_cmd, " ");
                String[] command = new String[st.countTokens()+2];
                if(System.getProperty("file.separator").equals("/")) {
                    command[0] = "/bin/sh";
                    command[1] = "-c";
                } else {
                    command[0] = "cmd.exe";
                    command[1] = "/C";
                }
                int i = 2;
                while(st.hasMoreTokens()) {
                    command[i++] = st.nextToken();
                }
                runProcess(command);
                return true;

            } else if(action.equals("edit")) {

                if(System.getProperty("file.separator").equals("/")) {
                    logln("Not implemented", ToolLogger.ERROR);
                    return true;
                }

                if(prev.length() > 0 ) {
                    String temp = editSQL(prev);
                    if(temp != null) {
                        prev = temp;
                    }
                } else {
                    logln("Nothing to save.", ToolLogger.ERROR);
                }
                return true;

            } else if(action.equals("clear")) {

                Util.clearScreen();
                return true;

            } else if(action.equals("showquery")) {

                if(prev.length() > 0) {
                    printQuery(prev);
                } else {
                    logln("No lines in SQL buffer", ToolLogger.RESULT);
                }
                return true;

            } else if(action.equals("previous")) {

                if(prev.length() > 0) {
                    query = prev;
                } else {
                    logln("No lines in SQL buffer", ToolLogger.RESULT);
                    return true;
                }

            } else if(action.equals("blank")) {

                return true;

            } else if(action.equals("unknown")) {

                logln("unknown command \"" + cmd + "\" - rest of line ignored.", ToolLogger.ERROR);
                return true;

            } else if(action.equals("connect")) {

                String temp = cmd.endsWith(";")?cmd.substring(0,cmd.length()-1):cmd;
                while(temp.indexOf("  ") > -1) {
                    temp = temp.replaceAll("  ", " ");
                }

                StringTokenizer st = new StringTokenizer(temp.trim(), " ");
                if(st.countTokens() == 1) {
                    user = dbinfo.inputUserName();
                    pass = dbinfo.inputPassword();
                } else if(st.countTokens() == 2) {
                    st.nextToken();
                    StringTokenizer st2 = new StringTokenizer(st.nextToken(), "/");
                    if(st2.countTokens() == 1) {
                        user = st2.nextToken();
                        pass = dbinfo.inputPassword();
                    } else if(st2.countTokens() == 2) {
                        user = st2.nextToken();
                        StringTokenizer st3 = new StringTokenizer(st2.nextToken(), "@");
                        if(st3.countTokens() == 1) {
                            pass = st3.nextToken();
                        } else if(st3.countTokens() == 2) {
                            if(dbinfo.getCanParseConnectionURL()) {
                                pass = st3.nextToken();
                                dbname = st3.nextToken();
                            } else {
                                logln("cannot specify DATABASE_NAME as far as ConnectionURL has 'DESCRIPTION'", ToolLogger.ERROR);
                                return true;
                            }
                        } else {
                            logln("Invalid option", ToolLogger.ERROR);
                            logln("Usage: CONN[ECT] <username>[/<password>][@<database_name>]", ToolLogger.INFO);
                            return true;
                        }
                    } else {
                        logln("Invalid option", ToolLogger.ERROR);
                        logln("Usage: CONN[ECT] <username>[/<password>][@<database_name>]", ToolLogger.INFO);
                        return true;
                    }
                } else {
                    temp = temp.trim().toLowerCase();
                    String opt = temp.substring(temp.indexOf(" ")+1);
                    if(opt.equals("/as sysdba") || opt.equals("/ as sysdba") || opt.equals("/as sysoper") || opt.equals("/ as sysoper")) {
                        logln("insufficient privileges", ToolLogger.ERROR);
                        logln("can't connect as sysdba or sysoper", ToolLogger.ERROR);
                        return true;
                    } else {
                        logln("Invalid option", ToolLogger.ERROR);
                        logln("Usage: CONN[ECT] <username>[/<password>][@<database_name>]", ToolLogger.INFO);
                        return true;
                    }
                }

                boolean prev_silent = ToolLogger.getSilent();
                ToolLogger.setSilent(true);
                closeConnection();
                ToolLogger.setSilent(prev_silent);

                try {
                	createConnection(false);
                } catch(Exception e) {
                	return false;
                }

                return true;

            } else if(action.equals("define")) {

                String temp = cmd.endsWith(";")?cmd.substring(0,cmd.length()-1):cmd;
                while(temp.indexOf("  ") > -1) {
                    temp = temp.replaceAll("  ", " ");
                }

                StringTokenizer st = new StringTokenizer(temp.trim(), " ");
                String first = st.nextToken().toLowerCase();

                if(Util.isIncludeEquals(first, "def", "ine")) {
                    if(!st.hasMoreTokens()) {
                        Enumeration<String> e = defines.keys();
                        String var;
                        String value;
                        int cnt = 0;
                        while(e.hasMoreElements()) {
                            var = e.nextElement();
                            value = defines.get(var);
                            logln("DEFINE " + var + "\t\t= \"" + value + "\"", ToolLogger.RESULT);
                            cnt++;
                        }
                        if(cnt < 1) {
                            logln("no define info", ToolLogger.RESULT);
                        }
                    } else {
                        String var = st.nextToken();
                        String ch = Util.getFirstSpecialChar(var);

                        if(ch != null) {
                            logln("Illegal variable name \"" + var + "\".", ToolLogger.ERROR);
                            return true;
                        }

                        if(!st.hasMoreTokens() || !st.nextToken().equals("=")) {
                            logln("DEFINE requires an equal sign (=)", ToolLogger.ERROR);
                            return true;
                        }

                        if(!st.hasMoreTokens()) {
                            logln("DEFINE requires a value following equal sign", ToolLogger.ERROR);
                            return true;
                        } else {
                            String vals = temp.trim().substring(temp.indexOf(" = ")+3).trim();
                            String val = st.nextToken();
                            if(vals.startsWith("\'") || vals.startsWith("\"")) {
                                String quot = vals.substring(0, 1);
                                int last_idx = vals.substring(1).lastIndexOf(quot);
                                if(last_idx < 0) {
                                    logln("string \"" + vals + "\" missing terminating quote (" + quot + ")", ToolLogger.ERROR);
                                    return true;
                                }
                                val = vals.substring(1).substring(0, last_idx);
                                if(!vals.substring(1).substring(last_idx+1).equals("")) {
                                    logln("string must end with quot (" + quot + ")", ToolLogger.ERROR);
                                    return true;
                                }
                            } else {
                                if(st.hasMoreTokens()) {
                                    logln("too many variables", ToolLogger.ERROR);
                                    return true;
                                }
                            }
                            defines.put(var.toUpperCase(), val);
                        }
                    }
                } else {
                    if(st.hasMoreTokens()) {
                        while(st.hasMoreTokens()) {
                            defines.remove(st.nextToken().toUpperCase());
                        }
                    } else {
                        logln("variable expected but not found", ToolLogger.ERROR);
                    }
                }

                return true;

            } else if(action.equals("setting")) {

                String temp = cmd.endsWith(";")?cmd.substring(0,cmd.length()-1):cmd;
                while(temp.indexOf("  ") > -1) {
                    temp = temp.replaceAll("  ", " ");
                }

                StringTokenizer st = new StringTokenizer(temp.trim().toLowerCase(), " ");
                if(st.countTokens() == 1) {
                    logln("PAGESIZE\t\t" + pageSize);
                    logln("LINESIZE\t\t" + lineSize);
                    logln("CHECKSIZE\t\t" + checkSize);
                    logln("TIMEOUT\t\t\t" + timeOut);
                    logln("HEADING\t\t\t" + ((showHead)?"ON":"OFF"));
                    logln("TIMING\t\t\t" + ((showTiming)?"ON":"OFF"));
                    logln("TIME\t\t\t" + ((showTime)?"ON":"OFF"));
                    logln("SCAN\t\t\t" + ((scanVar)?"ON":"OFF"));
                    logln("CHECK\t\t\t" + ((checkResultSet)?"ON":"OFF"));
                    logln("CHECKCONN\t\t" + RetainableConnection.CheckType.getNameByType(checkConnMethod));
                    logln("CHECKCONN_IDLETIME\t" + checkConnIdleTime);
                    return true;
                }
                st.nextToken();
                String subcmd;
                String p_size = null;
                String l_size = null;
                String c_size = null;
                String t_out = null;
                String s_head = null;
                String t_ming = null;
                String t_me = null;
                String s_var = null;
                String r_check = null;
                String c_method = null;
                String c_idle = null;
                boolean read = true;
                while(st.hasMoreTokens() && read) {
                    subcmd = st.nextToken();
                    if(Util.isIncludeEquals(subcmd, "pages", "ize")) {
                        String size = null;
                        size = (st.hasMoreTokens())?st.nextToken():"0";
                        if(!Util.isNumber(size)) {
                            logln("pagesize option not a valid number", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = null;
                            read = false;
                        } else {
                            p_size = size;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "lin", "esize")) {
                        String size = null;
                        size = (st.hasMoreTokens())?st.nextToken():null;
                        if(size == null || (size != null && Util.isNumber(size) && Integer.parseInt(size) < 1)) {
                            logln("linesize option out of range (1 through 32767)", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else if(!Util.isNumber(size)) {
                            logln("linesize option not a valid number", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            l_size = size;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "checks", "ize")) {
                        String size = null;
                        size = (st.hasMoreTokens())?st.nextToken():null;
                        if(size == null || (size != null && Util.isNumber(size) && Integer.parseInt(size) < 1)) {
                            logln("checksize option out of range", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else if(!Util.isNumber(size)) {
                            logln("checksize option not a valid number", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            c_size = size;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "timeo", "ut")) {
                        String to = null;
                        to = (st.hasMoreTokens())?st.nextToken():null;
                        if(to == null || !Util.isNumber(to)) {
                            logln("timeout option not a valid number", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            t_out = to;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "hea", "ding")) {
                        String heading = null;
                        heading = (st.hasMoreTokens())?st.nextToken():null;
                        if(heading == null || (!heading.equalsIgnoreCase("on") && !heading.equalsIgnoreCase("off"))) {
                            logln("heading must be set to ON or OFF", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            s_head = heading;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "timi", "ng")) {
                        String timing = null;
                        timing = (st.hasMoreTokens())?st.nextToken():null;
                        if(timing == null || (!timing.equalsIgnoreCase("on") && !timing.equalsIgnoreCase("off"))) {
                            logln("timing must be set to ON or OFF", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            t_ming = timing;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "ti", "me")) {
                        String time = null;
                        time = (st.hasMoreTokens())?st.nextToken():null;
                        if(time == null || (!time.equalsIgnoreCase("on") && !time.equalsIgnoreCase("off"))) {
                            logln("time must be set to ON or OFF", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            t_me = time;
                        }
                    } else if(subcmd.equals("scan")) {
                        String scan = null;
                        scan = (st.hasMoreTokens())?st.nextToken():null;
                        if(scan == null || (!scan.equalsIgnoreCase("on") && !scan.equalsIgnoreCase("off"))) {
                            logln("scan must be set to ON or OFF", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            s_var = scan;
                        }
                    } else if(subcmd.equals("check")) {
                        String check = null;
                        check = (st.hasMoreTokens())?st.nextToken():null;
                        if(check == null || (!check.equalsIgnoreCase("on") && !check.equalsIgnoreCase("off"))) {
                            logln("check must be set to ON or OFF", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            r_check = check;
                        }
                    } else if(subcmd.equals("checkconn")) {
                        String connmethod = null;
                        connmethod = (st.hasMoreTokens())?st.nextToken():null;
                        if(connmethod == null || RetainableConnection.CheckType.getTypeByName(connmethod) < 0) {
                            logln("checkconn must be set to one of OFF,FIRST_CONNECT,PRE_REQUEST,IDLE_CHECK,ALL", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                        	c_method = connmethod;
                        }
                    } else if(Util.isIncludeEquals(subcmd, "checkconn_i", "dletime")) {
                        String connidle = null;
                        connidle = (st.hasMoreTokens())?st.nextToken():null;
                        if(connidle == null || !Util.isNumber(connidle)) {
                            logln("checkconn_idletime option not a valid number", ToolLogger.ERROR);
                            p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                            read = false;
                        } else {
                            c_idle = connidle;
                        }
                    } else {
                        logln("unknown SET option \"" + subcmd + "\"", ToolLogger.ERROR);
                        p_size = l_size = c_size = t_out = s_head = t_ming = t_me = s_var = r_check = c_method = c_idle = null;
                        read = false;
                    }
                }

                if(p_size!=null) {
                    pageSize = Integer.parseInt(p_size);
                }
                if(l_size!=null) {
                    lineSize = Integer.parseInt(l_size);
                }
                if(c_size!=null) {
                    checkSize = Integer.parseInt(c_size);
                }
                if(t_out!=null) {
                    timeOut = Integer.parseInt(t_out);
                }
                if(s_head!=null) {
                    showHead = (s_head.trim().toLowerCase().equals("on"))?true:false;
                }
                if(t_ming!=null) {
                    showTiming = (t_ming.trim().toLowerCase().equals("on"))?true:false;
                }
                if(t_me!=null) {
                    showTime = (t_me.trim().toLowerCase().equals("on"))?true:false;
                }
                if(s_var!=null) {
                    scanVar = (s_var.trim().toLowerCase().equals("on"))?true:false;
                }
                if(r_check!=null) {
                    checkResultSet = (r_check.trim().toLowerCase().equals("on"))?true:false;
                }
                if(c_method!=null) {
                    checkConnMethod = RetainableConnection.CheckType.getTypeByName(c_method);
                    rcon.setCheckQueryType(checkConnMethod);
                    rcon.resetCheckQuery();
                }
                if(c_idle!=null) {
                    checkConnIdleTime = Integer.parseInt(c_idle);
                    rcon.setCheckQueryInterval(checkConnIdleTime);
                    rcon.resetCheckQuery();
                }

                return true;

            } else if(action.equals("column")) {

                String temp = cmd.endsWith(";")?cmd.substring(0,cmd.length()-1):cmd;
                while(temp.indexOf("  ") > -1) {
                    temp = temp.replaceAll("  ", " ");
                }

                StringTokenizer st = new StringTokenizer(temp.trim(), " ");
                st.nextToken();
                String colname;
                String subcmd;
                String format = null;
                String heading = null;
                String apply = null;
                String wrapped = null;
                boolean read = true;

                if(st.hasMoreTokens()) {
                    colname = st.nextToken().toUpperCase();
                    if(st.hasMoreTokens()) {
                        while(st.hasMoreTokens() && read) {
                            subcmd = st.nextToken().toLowerCase();
                            String temp2 = null;
                            if(Util.isIncludeEquals(subcmd, "for", "mat") || Util.isIncludeEquals(subcmd, "hea", "ding")) {
                                temp2 = (st.hasMoreTokens())?st.nextToken():null;
                                if(temp2 == null) {
                                    logln("string expected but not found", ToolLogger.ERROR);
                                    format = heading = apply = null;
                                    wrapped = null;
                                    read = false;
                                } else {
                                    if(subcmd.startsWith("for")) {
                                        if(checkFormat(temp2)) {
                                            format = temp2;
                                        } else {
                                            logln("Illegal FORMAT string \"" + temp2 + "\"", ToolLogger.ERROR);
                                            format = heading = apply = null;
                                            wrapped = null;
                                            read = false;
                                        }
                                    } else {
                                        if(temp2.startsWith("'")) {
                                            if(!temp2.endsWith("'")) {
                                                while(st.hasMoreTokens() && !temp2.endsWith("'")) {
                                                    temp2 += " " + st.nextToken();
                                                }
                                            }
                                            if(temp2.endsWith("'")) {
                                                heading = temp2.substring(1,temp2.length()-1);
                                            } else {
                                                logln("string \"" + temp2 + "\" missing terminating quote (')", ToolLogger.ERROR);
                                                format = heading = apply = null;
                                                wrapped = null;
                                                read = false;
                                            }
                                        } else if(temp2.startsWith("\"")) {
                                            if(!temp2.endsWith("\"")) {
                                                while(st.hasMoreTokens() && !temp2.endsWith("\"")) {
                                                    temp2 += " " + st.nextToken();
                                                }
                                            }
                                            if(temp2.endsWith("\"")) {
                                                heading = temp2.substring(1,temp2.length()-1);
                                            } else {
                                                logln("string \"" + temp2 + "\" missing terminating quote (\")", ToolLogger.ERROR);
                                                format = heading = apply = null;
                                                wrapped = null;
                                                read = false;
                                            }
                                        } else {
                                            heading = temp2;
                                        }
                                    }
                                }
                            } else if(subcmd.equals("on") || subcmd.equals("off")) {
                                apply = subcmd;
                            } else if(Util.isIncludeEquals(subcmd, "wra", "pped")) {
                                wrapped = "wrap";
                            } else if(Util.isIncludeEquals(subcmd, "tru", "ncated")) {
                                wrapped = "truncate";
                            } else if(Util.isIncludeEquals(subcmd, "cle", "ar")) {
                                Properties p = (Properties)setCols.get(colname);
                                if(p == null) {
                                    logln("COLUMN '" + colname + "' not defined", ToolLogger.ERROR);
                                    format = heading = apply = null;
                                    wrapped = null;
                                    read = false;
                                } else {
                                    setCols.remove(colname);
                                }
                            } else {
                                logln("unknown COLUMN option \"" + subcmd + "\"", ToolLogger.ERROR);
                                format = heading = apply = null;
                                wrapped = null;
                                read = false;
                            }
                        }

                        if(apply!=null) {
                            setColumnInfo(colname, SQLMinus.APPLY, apply);
                        }
                        if(heading!=null) {
                            setColumnInfo(colname, SQLMinus.HEADING, heading);
                        }
                        if(format!=null) {
                            setColumnInfo(colname, SQLMinus.FORMAT, format);
                        }
                        if(wrapped!=null) {
                            setColumnInfo(colname, SQLMinus.WRAPPED, wrapped);
                        }

                    } else {
                        printColumnSetting(colname);
                    }
                } else {
                    printAllColumnsSetting();
                }

                return true;

            } else if(action.equals("query")) {

                prev = query = cmd;

            } else if(action.equals("no")) {

                if(sqlbuffer.equals("") == false) {
                    prev = sqlbuffer;
                    sqlbuffer = "";
                }
                return true;

            } else if(action.equals("")) {
                return true;
            }


            if(rcon != null) {

                query = (scanVar)?replaceQueryVariable(query.trim()):query.trim();

                if(query.length() > 0) {

                    if(doTransactionWork(query)) {
                        return true;
                    }

                    try {
                        if(!dbinfo.getInputCharset().equals("")) {
                            query = new String(query.getBytes("8859_1"), dbinfo.getInputCharset());
                        }
                    } catch(UnsupportedEncodingException e) {
                        logln("Cannot execute query('" + query + "') with charset('" + dbinfo.getInputCharset() + "')", ToolLogger.ERROR);
                        logln("Please connect with valid charset.", ToolLogger.INFO);
                        System.exit(-1);
                    }

                    try {
                        rcon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                        if(timeOut > 0) {
                            rcon.setQueryTimeout(timeOut);
                        }
                        start = System.currentTimeMillis();
                        if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("show")) {
                            rs = rcon.executeQuery(query);
                            if(action.equals("describe")) {
                                printDescribe(rs.getMetaData());
                            } else {
                                printResultSet(rs);
                            }
                        } else {
                            int i = rcon.executeUpdate(query);
                            printResult(i, query);
                        }
                    } catch(SQLException se) {
                        if(action.equals("describe")) {
                            logln("schema object does not exist", ToolLogger.ERROR);
                        } else {
                            logln(se.getMessage().trim(), ToolLogger.ERROR);
                        }
                        if(showTiming) {
                            logln("Query execute Finish : " + (System.currentTimeMillis() - start) + " ms");
                        }
                        return true;
                    } catch(Exception e) {
                        logln(e.getMessage().trim(), ToolLogger.ERROR);
            	        DebugLogger.logln("Exception occurred in processAction().", e, DebugLogger.WARN);
                    } finally {
                        if( rs != null ) try { rs.close(); } catch(Exception e) {};
                        if( rcon != null ) try { rcon.closeStatement(); } catch(Exception e) {};
                    }

                    logln("", ToolLogger.RESULT);
                }
            } else {
                logln("Not connected to server.", ToolLogger.ERROR);
                try {
                	reconnect();
                } catch(Exception e) {
                    return false;
                }
            }

        }
        return true;
    }

}
