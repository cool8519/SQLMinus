package com.sds.tool.jdbc.sqlm.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

import com.sds.tool.util.DebugLogger;
import com.sds.tool.util.ObjectDataUtil;


public class RetainableConnection {
	
	private Connection con;
	private Statement stmt;
	private PreparedStatement pstmt;
	
	private String checkQuery = null;
	private int checkInterval = -1;
	private int checkType = 0;
	private CheckThread checkThread = null;
	
	public static class CheckType {
		public static final int OFF           = 0;
		public static final int FIRST_CONNECT = 1;
		public static final int IDLE_CHECK    = 2;
		public static final int PRE_REQUEST   = 3;
		public static final int ALL           = 4;
		public static String getNameByType(int type) {
			switch(type) {
				case 0: return "OFF";
				case 1: return "FIRST_CONNECT";
				case 2: return "IDLE_CHECK";
				case 3: return "PRE_REQUEST";
				case 4: return "ALL";
				default: return "UNKNOWN";
			}
		}
		public static int getTypeByName(String name) {
			if("OFF".equalsIgnoreCase(name)) return OFF;
			else if("FIRST_CONNECT".equalsIgnoreCase(name)) return FIRST_CONNECT;
			else if("IDLE_CHECK".equalsIgnoreCase(name)) return IDLE_CHECK;
			else if("PRE_REQUEST".equalsIgnoreCase(name)) return PRE_REQUEST;
			else if("ALL".equalsIgnoreCase(name)) return ALL;
			else return -1;
		}
	}
	
	public RetainableConnection(String conUrl, Properties props) throws Exception {
		DebugLogger.logln("Creating RetainableConnection : Connection URL=" + conUrl, DebugLogger.DEBUG);
		Properties prop_temp = (Properties)props.clone();
		prop_temp.remove("password");
		DebugLogger.logln("Creating RetainableConnection : Connection Properties=" + ObjectDataUtil.toString(prop_temp,false), DebugLogger.DEBUG);
		this.con = DriverManager.getConnection(conUrl, props);
        DebugLogger.logln("Successfully created RetainableConnection : " + con.toString(), DebugLogger.INFO);
	}
	
	public void setCheckQuerySQL(String sql) {
		this.checkQuery = sql;
        DebugLogger.logln("Set CheckQuery SQL : \"" + sql + "\"", DebugLogger.INFO);
	}
	
	public void setCheckQueryInterval(int intvl) {
		this.checkInterval = intvl;
        DebugLogger.logln("Set CheckQuery Interval : " + intvl, DebugLogger.INFO);
	}

	public void setCheckQueryType(int type) {
		this.checkType = ((type > CheckType.ALL) ? CheckType.OFF : type);
        DebugLogger.logln("Set CheckQuery Type : " + type, DebugLogger.INFO);
	}
	
	public void stopCheckThread() {
		if(checkThread != null) {
			checkThread.stopCheck();
			checkThread = null;
		}
	}
	
	public void resetCheckQuery() {
		stopCheckThread();
		if(checkType == CheckType.IDLE_CHECK || checkType == CheckType.ALL) {
			checkThread = new CheckThread(con, checkQuery, checkInterval);
			checkThread.startCheck();
		}
	}
	
	public Statement getStatement() {
		return stmt;
	}
	
	public PreparedStatement getPreparedStatement() {
		return pstmt;
	}
	
	public void setQueryTimeout(int timeout) throws SQLException {
		if(stmt != null) {
			stmt.setQueryTimeout(timeout);
		} else if(pstmt != null) {
			pstmt.setQueryTimeout(timeout);
		}
        DebugLogger.logln("Set CheckQuery Timeout : " + timeout, DebugLogger.INFO);
	}
	
	public void setAutoCommit(boolean autocommit) throws SQLException {
		con.setAutoCommit(autocommit);
        DebugLogger.logln("Set AutoCommit : " + autocommit, DebugLogger.INFO);
	}

	public Savepoint setSavepoint(String name) throws SQLException {
        DebugLogger.logln("Set Savepoint : " + name, DebugLogger.INFO);
		return con.setSavepoint(name);
	}

	public void commit() throws SQLException {
        DebugLogger.logln("Commit Transactions.", DebugLogger.WARN);
		con.commit();
	}
	
	public void rollback() throws SQLException {
        DebugLogger.logln("Rollback Transactions.", DebugLogger.WARN);
		con.rollback();
	}

	public void rollback(Savepoint sp) throws SQLException {
        DebugLogger.logln("Rollback Transactions with Savepoint : " + sp.getSavepointName(), DebugLogger.WARN);
		con.rollback(sp);
	}

	public void closeStatement() throws SQLException {
	    if(stmt != null) {
	        DebugLogger.logln("Closing Statement : " + stmt.toString(), DebugLogger.WARN);
	    	stmt.close();
	    	stmt = null;
	    }
	}

	public void closePreparedStatement() throws SQLException {
	    if(pstmt != null) { 
	        DebugLogger.logln("Closing PreparedStatement : " + pstmt.toString(), DebugLogger.WARN);
	    	pstmt.close();
	    	pstmt = null;
	    }
	}

	public void close() throws SQLException {
	    stopCheckThread();
	    if( stmt != null ) try { stmt.close(); } catch(Exception e) {};
	    if( pstmt != null ) try { pstmt.close(); } catch(Exception e) {};
	    
        DebugLogger.logln("Closing Connection : " + con.toString(), DebugLogger.WARN);
		con.close();
		con = null;
	}

	public  DatabaseMetaData getMetaData() throws SQLException {
        DebugLogger.logln("Getting MetaData from DB.", DebugLogger.DEBUG);
		return con.getMetaData();
	}
	
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		stmt = con.createStatement(resultSetType, resultSetConcurrency);
        DebugLogger.logln("Created Statement : " + stmt.toString(), DebugLogger.DEBUG);
		return stmt;
	}

	public QueryResult execute(String query) throws SQLException {
		if(checkType == CheckType.PRE_REQUEST || checkType == CheckType.ALL) {
			if(CheckThread.checkQuery(con, checkQuery)) {
				resetThreadTimer();
			}
		}
		QueryResult result = new QueryResult();
		result.executeAndSet(query, stmt!=null?stmt:pstmt);
        DebugLogger.logln("Executed " + (result.isSelect()?"Select":"Update") + " Query : \"" + query + "\"", DebugLogger.INFO);

		resetThreadTimer();
		return result;
	}
	
	public ResultSet executeQuery(String query) throws SQLException {
		if(checkType == CheckType.PRE_REQUEST || checkType == CheckType.ALL) {
			if(CheckThread.checkQuery(con, checkQuery)) {
				resetThreadTimer();
			}
		}
		ResultSet rs = null;
		if(stmt != null) {
			rs = stmt.executeQuery(query);
		} else if(pstmt != null) {
			rs = pstmt.executeQuery(query);
		}
        DebugLogger.logln("Executed Select Query : \"" + query + "\"", DebugLogger.INFO);

		resetThreadTimer();
		return rs;
	}

	public int executeUpdate(String query) throws SQLException {
		if(checkType == CheckType.PRE_REQUEST || checkType == CheckType.ALL) {
			if(CheckThread.checkQuery(con, checkQuery)) {
				resetThreadTimer();
			}
		}
		int i = -1;
		if(stmt != null) {
			i = stmt.executeUpdate(query);
		} else if(pstmt != null) {
			i = pstmt.executeUpdate(query);
		}
        DebugLogger.logln("Executed Update Query : \"" + query + "\"", DebugLogger.INFO);

        resetThreadTimer();
		return i;
	}
	
	private void resetThreadTimer() {
		if(checkThread != null) {
			checkThread.resetTimer();
		}
	}

}