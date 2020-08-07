/*******************************************************************************
 * Copyright 2017 vanilladb.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.vanilladb.core.storage.tx;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.vanilladb.core.sql.Constant;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.BufferMgr;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.concurrency.ConcurrencyMgr;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

/**
 * Provides transaction management for clients, ensuring that all transactions
 * are recoverable, and in general satisfy the ACID properties with specified
 * isolation level.
 */
public class Transaction {
	private static Logger logger = Logger.getLogger(Transaction.class.getName());

	private RecoveryMgr recoveryMgr;
	private ConcurrencyMgr concurMgr;
	private BufferMgr bufferMgr;
	private List<TransactionLifecycleListener> lifecycleListeners;
	private long txNum;
	private boolean readOnly;
	// [TODO] HashMap
	private ConcurrentHashMap<BlockId, Vector<Workspace>> workspace;

	/**
	 * Creates a new transaction and associates it with a recovery manager, a
	 * concurrency manager, and a buffer manager. This constructor depends on
	 * the file, log, and buffer managers from {@link VanillaDb}, which are
	 * created during system initialization. Thus this constructor cannot be
	 * called until {@link VanillaDb#init(String)} is called first.
	 * 
	 * @param txMgr
	 *            the transaction manager
	 * @param concurMgr
	 *            the associated concurrency manager
	 * @param recoveryMgr
	 *            the associated recovery manager
	 * @param bufferMgr
	 *            the associated buffer manager
	 * @param readOnly
	 *            is read-only mode
	 * @param txNum
	 *            the number of the transaction
	 */
	public Transaction(TransactionMgr txMgr, TransactionLifecycleListener concurMgr,
			TransactionLifecycleListener recoveryMgr, TransactionLifecycleListener bufferMgr, boolean readOnly,
			long txNum) {
		this.concurMgr = (ConcurrencyMgr) concurMgr;
		this.recoveryMgr = (RecoveryMgr) recoveryMgr;
		this.bufferMgr = (BufferMgr) bufferMgr;
		this.txNum = txNum;
		this.readOnly = readOnly;
		// [TODO] HashMap to record current Buffer, offset, newVal
		this.workspace = new ConcurrentHashMap<BlockId, Vector<Workspace>>();

		lifecycleListeners = new LinkedList<TransactionLifecycleListener>();
		// XXX: A transaction manager must be added before a recovery manager to
		// prevent the following scenario:
		// <COMMIT 1>
		// <NQCKPT 1,2>
		//
		// Although, it may create another scenario like this:
		// <NQCKPT 2>
		// <COMMIT 1>
		// But the current algorithm can still recovery correctly during this
		// scenario.
		addLifecycleListener(txMgr);
		/*
		 * A recover manager must be added before a concurrency manager. For
		 * example, if the transaction need to roll back, it must hold all locks
		 * until the recovery procedure complete.
		 */
		addLifecycleListener(recoveryMgr);
		addLifecycleListener(concurMgr);
		addLifecycleListener(bufferMgr);
	}
	
	public void printall() {
		System.out.println("Tx HashMap start");
		for (BlockId tempBlockId : workspace.keySet()) {
		    Vector<Workspace> workspaceSet = workspace.get(tempBlockId);
		    for (int i = 0 ; i < workspaceSet.size() ; i++) {
			    Workspace ws = (Workspace) workspaceSet.get(i);
			    System.out.println(tempBlockId + " " + ws.getOffset() + " " + ws.getVal());
		    }
		}
		System.out.println("Tx HashMap end");
	}

	public void addLifecycleListener(TransactionLifecycleListener listener) {
		lifecycleListeners.add(listener);
	}

	/**
	 * Commits the current transaction. Flushes all modified blocks (and their
	 * log records), writes and flushes a commit record to the log, releases all
	 * locks, and unpins any pinned blocks.
	 */
	public void commit() {
//		printall();
//		System.out.println(" TX - commit func ... ");
		this.writeBuff();
		for (TransactionLifecycleListener l : lifecycleListeners) {
			
			l.onTxCommit(this);
		}
			

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " committed");
	}

	/**
	 * Rolls back the current transaction. Undoes any modified values, flushes
	 * those blocks, writes and flushes a rollback record to the log, releases
	 * all locks, and unpins any pinned blocks.
	 */
	public void rollback() {
		for (TransactionLifecycleListener l : lifecycleListeners) {
//			this.writeBuff();
			l.onTxRollback(this);
		}

		if (logger.isLoggable(Level.FINE))
			logger.fine("transaction " + txNum + " rolled back");
		this.workspace.clear();
	}

	/**
	 * Finishes the current statement. Releases slocks obtained so far for
	 * repeatable read isolation level and does nothing in serializable
	 * isolation level. This method should be called after each SQL statement.
	 */
	public void endStatement() {
		for (TransactionLifecycleListener l : lifecycleListeners)
			l.onTxEndStatement(this);
	}

	public long getTransactionNumber() {
		return this.txNum;
	}

	public boolean isReadOnly() {
		return this.readOnly;
	}

	public RecoveryMgr recoveryMgr() {
		return recoveryMgr;
	}

	public ConcurrencyMgr concurrencyMgr() {
		return concurMgr;
	}

	public BufferMgr bufferMgr() {
		return bufferMgr;
	}
	
	public void processWorkspace(BlockId blk, int offset, Constant val, LogSeqNum lsn) {
//		System.out.println(" TX - processWorkspace func ... " + blk + " " + offset + " " + val);
		Vector<Workspace> workspaceSet = workspace.get(blk);
		if( workspaceSet == null) {
			workspaceSet = new Vector<Workspace>();
			workspaceSet.add(new Workspace(offset, val, lsn));
			workspace.put(blk, workspaceSet);
		} else {
			workspaceSet.add(new Workspace(offset, val, lsn));
			workspace.put(blk, workspaceSet);
		}
//		System.out.println("End tx processWorkspace ..." + blk + " " + offset + " " + val);
	}
	
	public Constant getVal(BlockId blk, int offset) {
		// System.out.println(" TX - getVal func - blk : " + blk + " offset : " + offset);
		Constant val = null;
		if (workspace.containsKey(blk) == false) {
//			System.out.println(" TX - getVal func - workspace.containsKey(blk) is false ... ");
			return val;
		}
		Vector<Workspace> workspaceSet = workspace.get(blk);
		for (int i = 0; i < workspaceSet.size() ; i++) {
			
			Workspace ws = (Workspace)workspaceSet.get(i);
			int curOffset = ws.getOffset();
			if (curOffset == offset) {
//				System.out.println(" TX - getVal func - val Exist : " + ws.getVal());
				val =  ws.getVal();
			}
				
		}
//		System.out.println(" TX - getVal func - blk : " + blk + " offset : " + offset);
		return val;
	}
	
	private void writeBuff() {
//		System.out.println(" TX - writeBuff func ... ");
		for (BlockId tempBlockId : workspace.keySet()) {
			this.concurrencyMgr().modifyBlock(tempBlockId);
			Buffer curBuff = this.bufferMgr().pin(tempBlockId);
		    Vector<Workspace> workspaceSet = workspace.get(tempBlockId);
		    for (int i = 0 ; i < workspaceSet.size() ; i++) {
			    Workspace ws = (Workspace) workspaceSet.get(i);
			    // Write through Disk ...
			    curBuff.setVal(ws.getOffset(), ws.getVal(), this.getTransactionNumber(), ws.getLsn());
		    }
		    this.concurrencyMgr().releaseBlock(tempBlockId);
		    this.bufferMgr().unpin(curBuff);
		}
		this.workspace.clear();
//		System.out.println(" Tx - writeBuff func exit ...");
	}
}
