package lasige.steeldb.comm;

public class OpcodeList {
	public static final int LOGIN_SEND = 100;
	public static final int LOGIN_OK = 101;
	public static final int LOGIN_ERROR = 102;
	
	public static final int EXECUTE = 200;
	public static final int EXECUTE_OK = 201;
	public static final int EXECUTE_ERROR = 202;
	public static final int EXECUTE_UPDATE = 210;
	public static final int EXECUTE_UPDATE_OK = 211;
	public static final int EXECUTE_UPDATE_ERROR = 212;
	public static final int EXECUTE_QUERY = 220;
	public static final int EXECUTE_QUERY_OK = 221;
	public static final int EXECUTE_QUERY_ERROR = 222;
	public static final int EXECUTE_BATCH = 230;
	public static final int EXECUTE_BATCH_OK = 231;
	public static final int EXECUTE_BATCH_ERROR = 232;
	
	public static final int COMMIT_MASTER_SEND = 310;
	public static final int COMMIT_MASTER_SEND_OK = 311;
	
	public static final int COMMIT_NON_MASTER_SEND = 320;
	public static final int COMMIT_NON_MASTER_SEND_OK = 321;
	
//	public static final int BEGIN = 350;
	public static final int COMMIT = 300;
	public static final int COMMIT_OK = 301;
	public static final int COMMIT_ERROR = 302;
	
	public static final int COMMIT_AUTO = 303;
	public static final int COMMIT_AUTO_OK = 304;
	public static final int COMMIT_AUTO_ERROR = 305;
	
	public static final int ROLLBACK_SEND = 400;
	public static final int ROLLBACK_OK = 410;
	public static final int ROLLBACK_MASTER_OK = 401;
	public static final int ROLLBACK_ERROR = 402;
	
	public static final int MASTER_CHANGE = 90;
	public static final int MASTER_CHANGE_OK = 91;
	public static final int MASTER_CHANGE_ERROR = 92;
	
	public static final int TIMEOUT = 93;
	public static final int ABORTED = 94;
	public static final int OLD_STATE = 95;
	public static final int GET_STATE = 96;
	public static final int STATE_REQUESTED = 97;
	
	public static final int OUT_OF_ORDER_MSG = 99;

	public static final int UNKNOWN_OP = -1;
	public static final int SET_FETCH_SIZE = 500;
	public static final int SET_MAX_ROWS = 501;
	public static final int CLOSE = 600;
	public static final int CLOSE_OK = 601;
	public static final int CLOSE_ERROR = 602;
	
	public static final int GET_DB_METADATA = 700;
	public static final int GET_DB_METADATA_OK = 701;
	public static final int GET_DB_METADATA_ERROR = 702;

}
