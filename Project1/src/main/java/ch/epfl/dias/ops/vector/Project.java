package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	// TODO: Add required structures
	VectorOperator child;
	int[] fieldNo;
	int vectorsize;

	public Project(VectorOperator child, int[] fieldNo) {

		// TODO: Implement
		this.child=child;
		this.fieldNo=fieldNo;
	}


	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next()
	{
		// TODO: Implement
		DBColumn[] temp = child.next();
		try{
			vectorsize = temp[0].attributes.length;
			DBColumn[] Return = new DBColumn[fieldNo.length];
			for(int i=0;i<fieldNo.length;i++)
			{
				Object[] return_attri = temp[fieldNo[i]].attributes;
				DataType type = temp[fieldNo[i]].type;
				Return[i] = new DBColumn(return_attri,type);
			}
			return Return;
		}catch (Exception e) {
			close();
			return null;
		}

	}

	@Override
	public void close() {
		// TODO: Implement
		child.close();
	}
}
