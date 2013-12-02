package lasige.steeldb.comm;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author mhsantos
 *
 */
public class RollbackRequest implements Serializable {
	
	private static final long serialVersionUID = 1738961460455194540L;
	private final boolean masterChanged;
	private final int oldMaster;
	
	private FinishTransactionRequest finishReq;
	
	public RollbackRequest(boolean masterChanged, int oldMaster, FinishTransactionRequest finishReq) {
		this.masterChanged = masterChanged;
		this.oldMaster = oldMaster;
		this.finishReq = finishReq;
	}
	
	public boolean isMasterChanged() {
		return masterChanged;
	}
	
	public int getOldMaster() {
		return oldMaster;
	}

	public FinishTransactionRequest getFinishReq() {
		return finishReq;
	}

}
