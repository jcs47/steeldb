package lasige.steeldb.jdbc;

import com.sun.rowset.internal.Row;

/**
 * Overrides equals() and hashCode() from Object.
 * The class row does not override such methods, so, it is hard o compare
 * results from different queries. In the row class, every time hashCode()
 * is called, even in the same object, it generates a different value. That
 * is so because it uses the Object's hashCode() method and that method
 * takes into account values like the memory address of the object.
 * 
 * @author mhsantos
 *
 */
public class BFTRow extends Row {

	public BFTRow(int arg0) {
		super(arg0);
	}

	private static final long serialVersionUID = 1511891954432772423L;
	
	@Override
	public boolean equals(Object o) {
		if(o == this)
			return true;
		if(!(o instanceof BFTRow))
			return false;
		Object[] thisObjs = getOrigRow();
		Row oRow = (Row)o;
		Object[] objs = oRow.getOrigRow();
		if(thisObjs.length != objs.length)
			return false;
		for(int i = 0; i < thisObjs.length; i++) {
			if(thisObjs[i] == null) {
				if(objs[i] != null)
					return false;
			} else
				if(!(thisObjs[i].equals(objs[i])))
					return false;
		}
		return true;
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		Object[] columns = super.getOrigRow();
		for(Object column : columns) {
			int hashCode = 0;
			if(column != null)
				hashCode = column.hashCode();
			result = 31 * result + hashCode;
		}
		return result;
	}
	
}
