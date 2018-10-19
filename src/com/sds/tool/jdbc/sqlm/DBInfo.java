package com.sds.tool.jdbc.sqlm;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;

import com.sds.tool.jdbc.sqlm.util.Util;
import com.sds.tool.util.DebugLogger;
import com.sds.tool.util.ObjectDataUtil;
import com.sds.tool.util.ToolLogger;


public class DBInfo {

    public static final int ORACLE    = 1;
    public static final int TIBERO    = 2;
    public static final int SQL2000   = 3;
    public static final int SQL2005   = 4;
    public static final int SYBASE    = 5;
    public static final int INFORMIX  = 6;
    public static final int DB2       = 7;
    public static final int MYSQL     = 8;
    public static final int POSTGRE   = 9;
    public static final int ALTIBASE  = 10;
    public static final int UNISQL    = 11;
    public static final int HSQL      = 12;
    public static final int POINTBASE = 13;
    public static final String[] DBMSName = { "Unknown",
                                              "Oracle",
                                              "Tibero",
                                              "MS-SQL 2000",
                                              "MS-SQL 2005",
                                              "Sybase",
                                              "Informix",
                                              "DB2",
                                              "MySQL",
                                              "PostgreSQL",
                                              "ALTIBASE",
                                              "UNISQL",
                                              "HSQLDB",
                                              "POINTBASE" };

    private int type = ORACLE;
    private boolean canParseConnectionURL = true;
    private String ip = "";
    private String port = "";
    private String dbname = "";
    private String user = null;
    private String pass = null;
    private String conUrl = null;
    private String inCharset = null;
    private String outCharset = "";
    private List<String> drivers = null;
    private String currentDir = System.getProperty("user.dir");
    private Hashtable<String,String> params = new Hashtable<String,String>();


    public DBInfo() {}


    private void logln(Object msg) {
        ToolLogger.logln(msg);
    }


    private void logln(Object msg, int level) {
        ToolLogger.logln(msg, level);
    }


    private String readLine(String prompt, int level) {
        return Util.readLine(prompt, level).trim();
    }


    public void setType(int type) {
        this.type = type;
        DebugLogger.logln("Database Type : " + DBMSName[type], DebugLogger.INFO);
    }


    public void setUsername(String user) {
        this.user = user;
    	DebugLogger.logln("Username : " + user, DebugLogger.INFO);
    }


    public void setPassword(String pass) {
        this.pass = pass;
    }


    public void setDBName(String dbname) {
        this.dbname = "";
        if(dbname != null && dbname.length() > 0) {
            this.dbname = dbname;
        	DebugLogger.logln("DB Name : " + dbname, DebugLogger.INFO);
        }
    }


    public void setInputCharset(String enc) {
        String in_enc = enc;
        String in_def = getDefaultCharset(true);
        if(enc.trim().equals("")) {
            in_enc = null;
        } else if(enc.trim().equals("-")) {
            in_enc = "";
        }
        this.inCharset = (in_enc == null)?in_def:in_enc;
        DebugLogger.logln("Input Charset : " + inCharset, DebugLogger.INFO);
    }


    public void setOutputCharset(String enc) {
        String out_enc = enc;
        String out_def = getDefaultCharset(false);
        if(enc.trim().equals("")) {
            out_enc = null;
        } else if(enc.trim().equals("-")) {
            out_enc = "";
        }
        this.outCharset = (out_enc == null)?out_def:out_enc;
        DebugLogger.logln("Output Charset : " + outCharset, DebugLogger.INFO);
    }


    public boolean setConnectionURL(String conUrl) {
        DebugLogger.logln("ConnectionURL : " + conUrl, DebugLogger.INFO);

        if(conUrl.trim().toLowerCase().indexOf("@(description=") > -1) {
            this.canParseConnectionURL = false;
            this.conUrl = conUrl;
            DebugLogger.logln("Can't parse ConnectionURL because description is included in it.", DebugLogger.WARN);
        } else {
            this.canParseConnectionURL = true;
            this.conUrl = conUrl;
            if(parseConnectionURL(conUrl) == false) {
                return false;
            }
        }
        return true;
    }

    
	public void setDrivers(List<String> drivers) {
        DebugLogger.logln("JDBC Drivers : " + ObjectDataUtil.toString(drivers,false), DebugLogger.INFO);
        this.drivers = drivers;
	}


    public void resetInfo(String user, String pass, String dbname) {
        this.user = "";
        this.pass = "";
        this.dbname = "";
        if(user != null && user.length() > 0)
            this.user = user;
        if(pass != null && pass.length() > 0)
            this.pass = pass;
        if(dbname != null && dbname.length() > 0)
            this.dbname = dbname;

        DebugLogger.logln("Cleared all connection info." + conUrl, DebugLogger.INFO);
    }


    public int getType() {
        return type;
    }


    public String getUserName() {
        return user;
    }


    public String getPassword() {
        return pass;
    }


    public String getDBName() {
        return dbname;
    }


    public String getInputCharset() {
        return inCharset;
    }


    public String getOutputCharset() {
        return outCharset;
    }


    public String getConnectionURL() {
    	if(conUrl == null) {
    		conUrl = makeConnectionURL();
    	}
        return conUrl;
    }


    public boolean getCanParseConnectionURL() {
        return canParseConnectionURL;
    }


    public void getInfo() {
        List<String> l = null;
        if(drivers == null) {
	        l = getLibraryList(currentDir);
        } else {
        	l = drivers;
        }
        if(l.size() < 1) {
            logln("No library to load.", ToolLogger.ERROR);
            DebugLogger.logln("No library to load.", DebugLogger.ERROR);
        	System.exit(-1);
        }
        DebugLogger.logln("Libraries to load : " + ObjectDataUtil.toString(l,false), DebugLogger.INFO);
        
        try {
            List<String> jar_list = addJDBCToClasspath(l);
            DebugLogger.logln("Successfully added libraries to the classpath : " + ObjectDataUtil.toString(jar_list,false), DebugLogger.INFO);
        } catch(Exception e) {
            logln(e.getMessage(), ToolLogger.ERROR);
            DebugLogger.logln(e.getMessage(), DebugLogger.ERROR);
        	System.exit(-1);
        }
    	
        if(conUrl == null) {
            inputType();
            inputAddr();
            inputPort();
            inputDBName();
            if(type == INFORMIX) {
                inputDBServer();
            }
        }

        if(user == null) {
            inputUserNameAndSet();
        }

        if(pass == null) {
            inputPasswordAndSet();
        }

        if(inCharset == null) {
            setCharset();
        }
    }


    private List<String> addJDBCToClasspath(List<String> l) throws Exception {
    	List<String> lst = new ArrayList<String>();
    	try {
    		final URLClassLoader loader = (URLClassLoader)ClassLoader.getSystemClassLoader();
    		final Method method = URLClassLoader.class.getDeclaredMethod("addURL", new Class[]{URL.class});
    		method.setAccessible(true);
    		for(String filename : l) {
				File f = new File(filename);
				if(f.exists()) {
	    			try {
	    				method.invoke(loader, new Object[]{f.toURI().toURL()});
	    				lst.add(filename);
	    				DebugLogger.logln("Added library to classpath : " + f.getCanonicalPath(), DebugLogger.DEBUG);
	    			} catch(Exception e) {
	    				DebugLogger.logln("Failed to add library to classpath : " + f.getCanonicalPath(), DebugLogger.ERROR);
	    			}
				} else {
    				DebugLogger.logln("Not found JDBC Driver : " + filename, DebugLogger.WARN);
				}
    		}
    	} catch(Exception e) {
			DebugLogger.logln("Error occurred while loading JDBC drivers.", DebugLogger.ERROR);
			throw e;
    	}
    	if(lst.size() < 1) {
    		throw new Exception("No JDBC Driver loaded.");
    	}
		return lst;
    }


	private void inputType() {
        logln("Choose DBMS", ToolLogger.INFO);
        logln("-----------", ToolLogger.INFO);
        for(int i = 1; i < DBMSName.length; i++) {
			logln(i + ") " + DBMSName[i], ToolLogger.INFO);
        }
        logln("", ToolLogger.INFO);

        String sel = "";
        int i_sel = 0;

        do {
            sel = readLine("Choose DBMS(Default:1): ", ToolLogger.INFO);
            if(sel.length() < 1) {
                sel = "1";
            }
            if(Util.isNumber(sel)) {
                i_sel = Integer.parseInt(sel);
                if(i_sel > DBMSName.length || i_sel < 1) {
                    logln("invalid number. try again...", ToolLogger.ERROR);
                }
            } else {
                logln("invalid number. try again...", ToolLogger.ERROR);
            }
        } while(!Util.isNumber(sel) || i_sel > DBMSName.length || i_sel < 1);

        type = i_sel;

        logln("", ToolLogger.INFO);

        DebugLogger.logln("Database Type : " + DBMSName[type], DebugLogger.INFO);
    }


    private void inputAddr() {
        do {
            ip = readLine("Enter address: ", ToolLogger.INFO);
            if(ip.length() < 1) {
                logln("Address cannot be null. try again...", ToolLogger.ERROR);
            }
        } while(ip.length() < 1);
        
    	DebugLogger.logln("Database IP : " + ip, DebugLogger.INFO);
    }


    private void inputPort() {
        do {
            port = readLine("Enter port: ", ToolLogger.INFO);
            if(port.length() < 1) {
                logln("Port cannot be null. try again...", ToolLogger.ERROR);
            } else if(!Util.isNumber(port)) {
                logln("Port must be a number. try again...", ToolLogger.ERROR);
            }
        } while(!Util.isNumber(port) || port.length() < 1);

    	DebugLogger.logln("Database Port : " + port, DebugLogger.INFO);
    }


    private void inputDBName() {
        dbname = readLine("Enter database: ", ToolLogger.INFO);        
    	DebugLogger.logln("DB Name : " + dbname, DebugLogger.INFO);
    }


    private void inputDBServer() {
        String inforsvr;
        if(type == INFORMIX) {
            inforsvr = readLine("Enter db-server: ", ToolLogger.INFO);
            params.put("INFORMIXSERVER", inforsvr);
        	DebugLogger.logln("DB Server : " + inforsvr, DebugLogger.INFO);
        }
    }


    private void inputUserNameAndSet() {
        user = inputUserName();
    	DebugLogger.logln("Username : " + user, DebugLogger.INFO);
    }


    public String inputUserName() {
        String u = "";
        do {
            u = readLine("Enter user-name: ", ToolLogger.INFO);
            if(u.length() < 1) {
                logln("UserName cannot be null. try again...", ToolLogger.ERROR);
            }
        } while(u.length() < 1);
        return ((u==null)?"":u.trim());
    }


    private void inputPasswordAndSet() {
        pass = inputPassword();
    }


    public String inputPassword() {
        String p = "";
        p = Util.readPassword("Enter password: ");
        return ((p==null)?"":p.trim());
    }


    public boolean isDBAUser() {
        switch(type) {
            case ORACLE    :
                if(user.equalsIgnoreCase("SYS"))
                    return true;
            default        :
                return false;
        }
    }


    public boolean parseConnectionURL(String conUrl) {
        int temp_type = 0;
        String temp_ip = "";
        String temp_port = "";
        String temp_dbname = "";
        String temp_user = "";
        String temp_pass = "";

        StringTokenizer st = new StringTokenizer(conUrl.trim(), ":");
        String token = st.nextToken();
        if(token == null || !token.equals("jdbc")) return false;
        token = st.nextToken();
        if(token.equals("oracle") || token.equals("tibero")) {
            temp_type = (token.equals("oracle"))?DBInfo.ORACLE:DBInfo.TIBERO;
            if(st.countTokens() != 4) return false;
            if(!st.nextToken("@").equals(":thin:")) return false;
            temp_ip = st.nextToken(":").substring(1);
            temp_port = st.nextToken();
            temp_dbname = st.nextToken();
        } else if(token.equals("microsoft") || token.equals("sqlserver")) {
            if(token.equals("microsoft")) {
                temp_type = DBInfo.SQL2000;
                token = st.nextToken();
            } else {
                temp_type = DBInfo.SQL2005;
            }
            temp_ip = st.nextToken();
            if(!temp_ip.startsWith("//")) return false;
            temp_ip = temp_ip.substring(2);
            temp_port = st.nextToken(";").substring(1);
            String temp = st.nextToken();
            st = new StringTokenizer(temp, ";");
            while(st.hasMoreTokens()) {
                StringTokenizer st2 = new StringTokenizer(st.nextToken(), "=");
                if(st2.countTokens() > 2) return false;
                String name = st2.nextToken();
                String value = (st2.hasMoreTokens())?st2.nextToken():"";
                if(name.equalsIgnoreCase("databasename")) {
                    temp_dbname = value;
                } else {
                    params.put(name, value);
                }
            }
        } else if(token.equals("sybase")) {
            temp_type = DBInfo.SYBASE;
            if(st.countTokens() != 3) return false;
            if(!st.nextToken().equals("Tds")) return false;
            temp_ip = st.nextToken();
            temp_port = st.nextToken("/").substring(1);
            temp_dbname = st.nextToken();
        } else if(token.equals("informix-sqli")) {
            temp_type = DBInfo.INFORMIX;
            if(st.countTokens() < 2) return false;
            temp_ip = st.nextToken();
            if(!temp_ip.startsWith("//")) return false;
            temp_ip = temp_ip.substring(2);
            temp_port = st.nextToken("/").substring(1);
            temp_dbname = st.nextToken(":").substring(1);
            if(st.countTokens() > 0) {
                StringTokenizer st_opts = new StringTokenizer(st.nextToken(), ";");
                while(st_opts.hasMoreTokens()) {
                    StringTokenizer st2 = new StringTokenizer(st_opts.nextToken(), "=");
                    if(st2.countTokens() > 2) return false;
                    String name = st2.nextToken();
                    String value = (st2.hasMoreTokens())?st2.nextToken():"";
                    if(name.equalsIgnoreCase("user")) {
                        temp_user = value;
                        user = temp_user;
                    } else if(name.equalsIgnoreCase("password")) {
                        temp_pass = value;
                        pass = temp_pass;
                    } else {
                        params.put(name, value);
                    }
                }
            }
        } else if(token.equals("db2")) {
            temp_type = DBInfo.DB2;
            if(st.countTokens() < 2) return false;
            temp_ip = st.nextToken();
            if(!temp_ip.startsWith("//")) return false;
            temp_ip = temp_ip.substring(2);
            temp_port = st.nextToken("/").substring(1);
            temp_dbname = st.nextToken(":").substring(1);
            if(st.countTokens() > 0) {
                StringTokenizer st_opts = new StringTokenizer(st.nextToken(), ";");
                while(st_opts.hasMoreTokens()) {
                    StringTokenizer st2 = new StringTokenizer(st_opts.nextToken(), "=");
                    if(st2.countTokens() > 2) return false;
                    String name = st2.nextToken();
                    String value = (st2.hasMoreTokens())?st2.nextToken():"";
                    params.put(name, value);
                }
            }
        } else if(token.equals("mysql") || token.equals("postgresql") || token.equals("Altibase")) {
            if(token.equals("mysql")) {
                temp_type = DBInfo.MYSQL;
            } else if(token.equals("postgresql")) {
                temp_type = DBInfo.POSTGRE;
            } else if(token.equals("Altibase")) {
                temp_type = DBInfo.ALTIBASE;
            }
            if(st.countTokens() != 2) return false;
            temp_ip = st.nextToken();
            if(!temp_ip.startsWith("//")) return false;
            temp_ip = temp_ip.substring(2);
            temp_port = st.nextToken("/").substring(1);
            temp_dbname = st.nextToken("?").substring(1);
            if(st.countTokens() > 0) {
                StringTokenizer st_opts = new StringTokenizer(st.nextToken(), "&");
                while(st_opts.hasMoreTokens()) {
                    StringTokenizer st2 = new StringTokenizer(st_opts.nextToken(), "=");
                    if(st2.countTokens() > 2) return false;
                    String name = st2.nextToken();
                    String value = (st2.hasMoreTokens())?st2.nextToken():"";
                    params.put(name, value);
                }
            }
        } else if(token.equals("unisql")) {
            temp_type = DBInfo.UNISQL;
            if(st.countTokens() != 3) return false;
            temp_ip = st.nextToken();
            temp_port = st.nextToken();
            temp_dbname = st.nextToken("#").substring(1);
            int idx = temp_dbname.indexOf(':');
            if(idx < 1 || !temp_dbname.substring(idx).equals(":::")) return false;
            temp_dbname = temp_dbname.substring(0, idx);
        } else if(token.equals("hsqldb") || token.equals("pointbase")) {
            if(token.equals("hsqldb")) {
                temp_type = DBInfo.HSQL;
            } else if(token.equals("pointbase")) {
                temp_type = DBInfo.POINTBASE;
            }
            if(st.countTokens() != 3) return false;
            if(temp_type == DBInfo.HSQL) {
                if(!st.nextToken().equals("hsql")) return false;
            } else if(temp_type == DBInfo.POINTBASE) {
                if(!st.nextToken().equals("server")) return false;
            }
            temp_ip = st.nextToken();
            if(!temp_ip.startsWith("//")) return false;
            temp_ip = temp_ip.substring(2);
            temp_port = st.nextToken("/").substring(1);
            if(st.countTokens() > 0) {
                temp_dbname = st.nextToken();
            }
        } else {
            return false;
        }

        type = temp_type;
        ip = temp_ip;
        port = temp_port;
        dbname = temp_dbname;

        DebugLogger.logln("Successfully parse ConnectionURL.", DebugLogger.DEBUG);
        DebugLogger.logln("Database Type : " + DBMSName[type], DebugLogger.INFO);
    	DebugLogger.logln("Database IP : " + ip, DebugLogger.INFO);
    	DebugLogger.logln("Database Port : " + port, DebugLogger.INFO);
    	DebugLogger.logln("DB Name : " + dbname, DebugLogger.INFO);

        return true;
    }


    public String makeConnectionURL() {
        String conStr = "";
        switch(type) {
            case ORACLE    :
                conStr = "jdbc:oracle:thin:@" + ip + ":" + port + ":" + dbname;
                break;
            case TIBERO    :
                conStr = "jdbc:tibero:thin:@" + ip + ":" + port + ":" + dbname;
                break;
            case SQL2000   :
                conStr = "jdbc:microsoft:sqlserver://" + ip + ":" + port + ";DatabaseName=" + dbname + Util.getParameters(params, ";", ";");
                break;
            case SQL2005   :
                conStr = "jdbc:sqlserver://" + ip + ":" + port + ";DatabaseName=" + dbname + Util.getParameters(params, ";", ";");
                break;
            case SYBASE    :
                conStr = "jdbc:sybase:Tds:" + ip + ":" + port + "/" + dbname;
                break;
            case INFORMIX  :
                conStr = "jdbc:informix-sqli://" + ip + ":" + port + "/" + dbname + Util.getParameters(params, ":", ";", true);
                break;
            case DB2       :
                conStr = "jdbc:db2://" + ip + ":" + port + "/" + dbname + Util.getParameters(params, ":", ";");
                break;
            case MYSQL     :
                conStr = "jdbc:mysql://" + ip + ":" + port + "/" + dbname + Util.getParameters(params, "?", "&");
                break;
            case POSTGRE   :
                conStr = "jdbc:postgresql://" + ip + ":" + port + "/" + dbname + Util.getParameters(params, "?", "&");
                break;
            case ALTIBASE  :
                conStr = "jdbc:Altibase://" + ip + ":" + port + "/" + dbname + Util.getParameters(params, "?", "&");
                break;
            case UNISQL    :
                conStr = "jdbc:unisql:" + ip + ":" + port + ":" + dbname + ":::";
                break;
            case HSQL      :
                conStr = "jdbc:hsqldb:hsql://" + ip + ":" + port + "/" + dbname;
                break;
            case POINTBASE :
                conStr = "jdbc:pointbase:server://" + ip + ":" + port + "/" + dbname;
                break;
        }
        DebugLogger.logln("Generated ConnectionURL : " + conStr, DebugLogger.INFO);
        return conStr;
    }


    public void loadJDBCDriver() {
        DebugLogger.logln("Loading JDBC Driver Class.", DebugLogger.DEBUG);
        String jdbcClass = "";
        try {
            switch(type) {
                case ORACLE    :
                    jdbcClass = "oracle.jdbc.driver.OracleDriver";
                    Class.forName(jdbcClass);
                    break;
                case TIBERO    :
                    try {
                        jdbcClass = "com.tmax.tibero.jdbc.TbDriver";
                        Class.forName(jdbcClass);
                    } catch(ClassNotFoundException ex) {
                        jdbcClass = "com.tmax.jdbc.tibero.TbrDriver";
                        Class.forName(jdbcClass);
                    }
                    break;
                case SQL2000   :
                    jdbcClass = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
                    Class.forName(jdbcClass);
                    break;
                case SQL2005   :
                    jdbcClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                    Class.forName(jdbcClass);
                    break;
                case SYBASE    :
                    jdbcClass = "com.sybase.jdbc2.jdbc.SybDriver";
                    Class.forName(jdbcClass);
                    break;
                case INFORMIX  :
                    jdbcClass = "com.informix.jdbc.IfxDriver";
                    Class.forName(jdbcClass);
                    break;
                case DB2       :
                    try {
                        jdbcClass = "com.ibm.db2.jcc.DB2Driver";
                        Class.forName(jdbcClass);
                    } catch(ClassNotFoundException ex) {
                        jdbcClass = "COM.ibm.db2.jdbc.net.DB2Driver";
                        Class.forName(jdbcClass);
                    }
                    break;
                case MYSQL     :
                    try {
                        jdbcClass = "com.mysql.jdbc.Driver";
                        Class.forName(jdbcClass);
                    } catch(ClassNotFoundException ex) {
                        jdbcClass = "org.gjt.mm.mysql.Driver";
                        Class.forName(jdbcClass);
                    }
                    break;
                case POSTGRE   :
                    try {
                        jdbcClass = "org.postgresql.Driver";
                        Class.forName(jdbcClass);
                    } catch(ClassNotFoundException ex) {
                        jdbcClass = "postgresql.Driver";
                        Class.forName(jdbcClass);
                    }
                    break;
                case ALTIBASE  :
                    jdbcClass = "Altibase.jdbc.driver.AltibaseDriver";
                    Class.forName(jdbcClass);
                    break;
                case UNISQL    :
                    jdbcClass = "unisql.jdbc.driver.UniSQLDriver";
                    Class.forName(jdbcClass);
                    break;
                case HSQL      :
                    jdbcClass = "org.hsqldb.jdbcDriver";
                    Class.forName(jdbcClass);
                    break;
                case POINTBASE :
                    jdbcClass = "com.pointbase.jdbc.jdbcUniversalDriver";
                    Class.forName(jdbcClass);
                    break;
            }
            DebugLogger.logln("Successfully loaded JDBC Driver Class : " + jdbcClass, DebugLogger.INFO);
        } catch(Exception e) {
            DebugLogger.logln("Failed to load JDBC Driver Class : " + jdbcClass, e, DebugLogger.ERROR);
            System.exit(-1);
        }

        logln("JDBC Driver Class: " + jdbcClass, ToolLogger.INFO);
    }

    public String getLibraryNames(List<String> l) {
        StringBuffer sb = new StringBuffer("");
        for(int i = 0; i < l.size(); i++) {
            sb.append((String)l.get(i));
            sb.append(" ");
        }
        return sb.toString().trim();
    }

    
    public String getCheckQuery() {
        String query = null;
        switch(getType()) {
            case ORACLE    :
            case TIBERO    :
            case ALTIBASE  :
            	query = "SELECT 1 FROM DUAL"; break;
            case SQL2000   :
            case SQL2005   :
            case POSTGRE   :
            case MYSQL     :
            	query = "SELECT 1"; break;
            case SYBASE    :
            	query = "SELECT getdate()"; break;
            case INFORMIX  :
            	query = "SELECT CURRENT FROM systables WHERE tabid=1"; break;
            case DB2       :
            	query = "SELECT current date FROM SYSIBM.SYSDUMMY1"; break;
            case UNISQL    :
            	query = "SELECT 1 FROM DB_ROOT"; break;
            case HSQL      :
            	query = "SELECT COUNT(1) FROM SYSTEM_CATALOGS"; break;
            case POINTBASE :
            default:
        }
        return query;
    }


    public String getLSQuery(String obj) {
        int type = getType();
        String query = "";
        if(obj == null) {
            if(type == ORACLE || type == TIBERO)
                query = "select TABLE_NAME from USER_TABLES";
            else if(type == SQL2000 || type == SQL2005)
                query = "select TABLE_NAME from INFORMATION_SCHEMA.TABLES";
            else if(type == MYSQL)
                query = "show tables";
        } else {
            if(type == ORACLE || type == TIBERO) {
                if(obj.equalsIgnoreCase("index")) {
                    query = "select INDEX_NAME from USER_INDEXES";
                } else if(obj.equalsIgnoreCase("procedure")) {
                    query = "select OBJECT_NAME NAME from USER_PROCEDURES";
                } else if(obj.equalsIgnoreCase("sequence")) {
                    query = "select SEQUENCE_NAME from USER_SEQUENCES";
                } else if(obj.equalsIgnoreCase("synonym")) {
                    query = "select SYNONYM_NAME from USER_SYNONYMS";
                } else if(obj.equalsIgnoreCase("table")) {
                    query = "select TABLE_NAME from USER_TABLES";
                } else if(obj.equalsIgnoreCase("view")) {
                    query = "select VIEW_NAME from USER_VIEWS";
                } else if(obj.equalsIgnoreCase("tablespace")) {
                    query = "select TABLESPACE_NAME from USER_TABLESPACES";
                } else if(obj.equalsIgnoreCase("trigger")) {
                    query = "select TRIGGER_NAME from USER_TRIGGERS";
                } else if(obj.equalsIgnoreCase("user")) {
                    query = "select USERNAME from USER_USERS";
                } else {
                    logln("Invalid object name.", ToolLogger.ERROR);
                }
            }
        }
        return query;
    }


    public String getDESCQuery(String table) {
        int type = getType();
        String query = "";

        if(type == ORACLE || type == TIBERO)
            query = "select * from " + table + "  where rownum = 1";
        else if(type == SQL2000 || type == SQL2005)
            query = "select top 1 * from " + table;
        else if(type == MYSQL)
            query = "select * from " + table + " limit 1";

        return query;
    }


    private void setCharset() {
        String in_enc;
        String out_enc;

        do {
            in_enc = readLine("Input Charset(Default:" + getDefaultCharset(true) + ", \"-\" for no encoding): ", ToolLogger.INFO);
            setInputCharset(in_enc);
        } while(!inCharset.equals("") && !checkCharset(inCharset));

        do {
            out_enc = readLine("Output Charset(Default:" + getDefaultCharset(false) + ", \"-\" for no encoding): ", ToolLogger.INFO);
            setOutputCharset(out_enc);
        } while(!outCharset.equals("") && !checkCharset(outCharset));

        logln("", ToolLogger.INFO);
    }


    private boolean checkCharset(String charset) {
        if(Charset.isSupported(charset)) {
            return true;
        } else {
            logln("Unsupported Charset name. try again...", ToolLogger.ERROR);
            return false;
        }
    }


    private String getDefaultCharset(boolean input) {
        String def = "";
        if(input) {
            switch(type) {
                case ORACLE    :
                case TIBERO    :
                case SQL2000   :
                case SQL2005   :
                case MYSQL     :
                    def = "EUC-KR";
                    break;
                case SYBASE    :
                case INFORMIX  :
                case DB2       :
                case POSTGRE   :
                case ALTIBASE  :
                case UNISQL    :
                case HSQL      :
                case POINTBASE :
            }
        } else {
            switch(type) {
                case MYSQL     :
                    def = "EUC-KR";
                    break;
                case ORACLE    :
                case TIBERO    :
                case SQL2000   :
                case SQL2005   :
                case SYBASE    :
                case INFORMIX  :
                case DB2       :
                case POSTGRE   :
                case ALTIBASE  :
                case UNISQL    :
                case HSQL      :
                case POINTBASE :
            }
        }
        return def;
    }


    public List<String> getLibraryList(String dir) {
        File targetPath = new File(dir);
        if(targetPath.isDirectory()) {
            String fs = System.getProperty("file.separator");
            String ext = "";
            String[] fl = targetPath.list();
            File tmpFile = null;
            List<String> flist = new ArrayList<String>();
            for(int i = 0; i < fl.length; i++) {
                ext = fl[i].substring(fl[i].lastIndexOf(".")+1).toLowerCase();
                tmpFile = new File(targetPath.getAbsolutePath() + fs + fl[i]);
                if((ext.equals("jar") || ext.equals("zip")) && tmpFile.isFile()) {
                    flist.add(fl[i]);
                }
            }
            DebugLogger.logln("Libraries in current directory : " + ObjectDataUtil.toString(flist,false), DebugLogger.DEBUG);
            return flist;
        } else {
            return null;
        }


    }


    public void printInfo() {
        logln("------------------------------");
        logln(" Current Information");
        logln("------------------------------");
        logln("   DBMS Type : " + DBMSName[type]);
        logln("   Connection Info");
        logln("     - IP : " + ((ip.equals(""))?"Unknown":ip));
        logln("     - PORT : " + ((port.equals(""))?"Unknown":port));
        logln("     - DBNAME : " + ((dbname.equals(""))?"Unknown":dbname));
        logln("     - USER : " + user);
        logln("     - PASS : " + Util.getPasswordString(pass));
        logln("     - PARAMETERS : " + Util.getParameters(params, "", ","));
        logln("     - Connection URL : " + getConnectionURL());
        logln("   Character Set");
        logln("     - IN : " + inCharset);
        logln("     - OUT : " + outCharset);
        logln("");
    }


}
