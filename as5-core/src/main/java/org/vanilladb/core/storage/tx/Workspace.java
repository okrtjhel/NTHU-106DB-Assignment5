package org.vanilladb.core.storage.tx;

import org.vanilladb.core.sql.Constant;
//import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.log.LogSeqNum;


public class Workspace {
	private RecordPage logRp;
	// private Buffer logBuff;
	private int offset;
	private Constant val;
	private LogSeqNum lsn;
	
	public Workspace (int offset, Constant val, LogSeqNum lsn) {
		this.offset = offset;
		this.val = val;
		this.lsn = lsn;
	}
	
	public RecordPage getRecordPage() {
		System.out.println("In logRp : XXX ");
		System.out.println(this.logRp);
		return this.logRp;
	}
	
	public int getOffset() {
		return this.offset;
	}
	
	public Constant getVal() {
		return this.val;
	}
	
	public LogSeqNum getLsn() {
		return this.lsn;
	}
	
	public void setVal(Constant val) {
		this.val = val;
	}
	
	public void getInfo() {
		System.out.println(" --- Workspace --- ");
		System.out.println(" Workspace - lsn : " + this.lsn);
		System.out.println(" Workspace - val : " + this.val);
		System.out.println(" Workspace - offset : " + this.offset);
		System.out.println(" --- Workspace --- ");
	}
}
