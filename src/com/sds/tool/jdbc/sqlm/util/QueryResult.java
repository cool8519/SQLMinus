package com.sds.tool.jdbc.sqlm.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryResult {
	
	private String query = null;
	private ResultSet resultSet = null;
	private int updateCount = -1;
	private boolean isSelect = true;
	
	public QueryResult() {}

	public QueryResult(String query) {
		this.query = query;
	}
	
	public QueryResult(String query, ResultSet resultSet, int updateCount, boolean isSelect) {
		this.query = query;
		this.resultSet = resultSet;
		this.updateCount = updateCount;
		this.isSelect = isSelect;
	}

	public QueryResult(String query, Statement stmt) throws SQLException {
		this.query = query;
		setResult(stmt);
	}
	
	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	public int getUpdateCount() {
		return updateCount;
	}

	public void setCount(int updateCount) {
		this.updateCount = updateCount;
	}

	public boolean isSelect() {
		return isSelect;
	}

	public void setSelect(boolean isSelect) {
		this.isSelect = isSelect;
	}
	
	public void setResult(Statement stmt) throws SQLException {
		this.resultSet = stmt.getResultSet();
		this.updateCount = stmt.getUpdateCount();
		this.isSelect = this.resultSet!=null;
	}

	public void executeAndSet(String query, Statement stmt) throws SQLException {
		this.query = query;
		if(stmt.execute(query)) {
			this.resultSet = stmt.getResultSet();
			this.isSelect = true;
		} else {
			this.updateCount = stmt.getUpdateCount();
			this.isSelect = false;
		}
	}

}
