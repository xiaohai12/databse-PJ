package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.DBColumn;

public class Project implements BlockOperator {

	// TODO: Add required structures
	BlockOperator child;
	int[] columns;

	public Project(BlockOperator child, int[] columns) {
		// TODO: Implement
		this.child = child;
		this.columns = columns;
	}

	public DBColumn[] execute() {
		// TODO: Implement
		DBColumn[] temp = child.execute();
		try{
			DBColumn[] Return = new DBColumn[columns.length];
			for(int i=0;i<columns.length;i++)
			{
				Return[i]=temp[columns[i]];
			}
			return Return;
		}catch (Exception e){
			return null;
		}

	}
}
