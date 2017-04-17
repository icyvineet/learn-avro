package com.vineet.model;

import java.util.List;

public class HBaseRowData {
	private String rowKey;
	private String columnFamily;
	private List<Column> columns;

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public HBaseRowData(String rowKey, String columnFamily, List<Column> columns) {
		super();
		this.rowKey = rowKey;
		this.columnFamily = columnFamily;
		this.columns = columns;
	}

	public HBaseRowData() {
		super();
	}

	@Override
	public String toString() {
		return "HBaseRowData [rowKey=" + rowKey + ", columnFamily=" + columnFamily + ", columns=" + columns + "]";
	}

}
