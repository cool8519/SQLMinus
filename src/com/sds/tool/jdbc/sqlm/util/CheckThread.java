package com.sds.tool.jdbc.sqlm.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.sds.tool.util.DebugLogger;


class CheckThread extends Thread {
	
	private int period = 1;
	
	private Connection con;
	private String query;
	private int maxidle;
	private boolean flag;
	private long recent;


	CheckThread(Connection con, String query, int maxidle) {
		this.con = con;
		this.query = query;
		this.maxidle = maxidle;
		this.period = maxidle;
        DebugLogger.logln("Created CheckThread : con=" + con.toString() + ", query=\"" + query + "\", maxidle=" + maxidle + ", period=" + period, DebugLogger.DEBUG);
	}

	void startCheck() {
		this.flag = true;
		start();
	}

	synchronized void stopCheck() {
		this.flag = false;
		notify();
	}

	static boolean checkQuery(Connection con, String query) {
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.executeQuery(query);
	        DebugLogger.logln("Sent CheckQuery : \"" + query + "\"", DebugLogger.DEBUG);
	        return true;
		} catch(Exception e) {
			DebugLogger.logln("Failed to execute CheckQuery : " + e.getMessage(), DebugLogger.DEBUG);
			return false;
		} finally {
			if(stmt != null) try { stmt.close(); } catch(SQLException e) {}
		}
	}
	
	boolean checkQuery() {
        DebugLogger.logln("Sending CheckQuery by timer.", DebugLogger.DEBUG);
		return checkQuery(con, query);
	}
	
	public void resetTimer() {
		recent = System.currentTimeMillis();
        DebugLogger.logln("Reset CheckQuery Timer : " + recent, DebugLogger.DEBUG);
	}

	public void run() {
        DebugLogger.logln("Started CheckQuery Thread.", DebugLogger.INFO);

        resetTimer();
		try {
			while(flag) {
				synchronized(this) {
					wait(period*1000);
				}
				if(flag && (System.currentTimeMillis()-recent > maxidle*1000L)) {
					if(checkQuery()) {
						resetTimer();
					}
				}
			}
		} catch(Exception e) {
	        DebugLogger.logln("Exception occurred in CheckThread. Thread will be stopped.", e, DebugLogger.ERROR);
		}

        DebugLogger.logln("Stopped CheckQuery Thread.", DebugLogger.INFO);
	}
	
}