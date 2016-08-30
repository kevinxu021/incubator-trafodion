package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import java.io.Serializable;

import java.util.List;

public class EsgynMResult implements io.ampool.monarch.table.MResult, java.io.Serializable {

    private byte[] m_rowId;

    private java.lang.Long m_rowTimeStamp;

    private java.util.List<io.ampool.monarch.table.MCell> m_cells;

    private boolean m_deleted_flag;

    EsgynMResult(byte[] p_rowId) {
	m_rowId = p_rowId;
	m_deleted_flag = false;
    }

    public void setCells(java.util.List<io.ampool.monarch.table.MCell> p_cells) {
	m_cells = p_cells;
    }

    public java.util.List<io.ampool.monarch.table.MCell> getCells() {
	return m_cells;
    }
    
    public int size() {
	if (m_cells != null) {
	    return m_cells.size();
	}

	return 0;
    }
    
    public boolean isEmpty() {
	return (m_cells.size() <= 0);
    }

    public void setRowTimeStamp(java.lang.Long pv_rowTimeStamp) {
	m_rowTimeStamp = pv_rowTimeStamp;
    }

    public java.lang.Long getRowTimeStamp() {
	return m_rowTimeStamp;
    }

    public byte[] getRowId() {
	return m_rowId;
    }

    public int compareTo(io.ampool.monarch.table.MResult p_result) {
	Bytes.ByteArrayComparator lv_bc = new Bytes.ByteArrayComparator();

	return lv_bc.compare(this.m_rowId, 
			  p_result.getRowId());
	
    }

    public void setDeletedFlag(boolean pv_flag) {
	m_deleted_flag = pv_flag;
    }

    public boolean getDeletedFlag() {
	return m_deleted_flag;
    }

    public String toString() {
	StringBuilder lv_s = new StringBuilder();
	lv_s.append("EsgynMResult [");
	lv_s.append("rowkey: " + new String(getRowId()));
	lv_s.append(", #cells: " + size());
	lv_s.append(", timestamp: " + getRowTimeStamp());
	lv_s.append("]");

	return lv_s.toString();
    }

}
