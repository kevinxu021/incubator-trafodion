package io.esgyn.coprocessor;

import io.ampool.monarch.table.*;

import org.apache.hadoop.hbase.client.Durability;

public class EsgynMPut extends MPut {

    Durability m_durability = Durability.USE_DEFAULT;

    public EsgynMPut(byte[] rowKey) {
	super(rowKey);
    }

    byte[] getRow() {
	return super.getRowKey();
    }

    java.util.Map<io.ampool.monarch.table.internal.ByteArrayKey,java.lang.Object> getFamilyCellMap() 
    {
	return super.getColumnValueMap();
    }

    public void setDurability(Durability d) {
	m_durability = d;
    }

    public Durability getDurability() {
	return m_durability;
    }

    public String toString() {
	StringBuilder lv_s = new StringBuilder();
	lv_s.append("MPut.getColumnValueMap().size(): ");
	lv_s.append(super.getColumnValueMap().size());
	lv_s.append(", rowkey: ");
	lv_s.append(new String(super.getRowKey()));
	return lv_s.toString();
    }
}
