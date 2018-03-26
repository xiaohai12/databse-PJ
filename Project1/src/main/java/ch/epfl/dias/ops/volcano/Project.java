package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator child;
	int[] fieldNo;


	public Project(VolcanoOperator child, int[] fieldNo) {
		// TODO: Implement
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();


	}

	@Override
	public DBTuple next() {

		// TODO: Implement
		DBTuple temp = child.next();
		try
		{
			Object[] data = new Object[fieldNo.length];
			DataType[] Type = new DataType[fieldNo.length];
			for(int num=0;num<fieldNo.length;num++)
			{
				Type[num] = temp.types[fieldNo[num]];
				data[num] = temp.fields[fieldNo[num]];
			}

			DBTuple Return = new DBTuple(data,Type);

			return Return;
		}catch (Exception e){
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
