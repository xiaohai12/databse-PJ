package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;

public class Scan implements BlockOperator {

	// TODO: Add required structures
	ColumnStore store;

	ArrayList<DBColumn> buff;

	public Scan(ColumnStore store) {
		// TODO: Implement
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		// TODO: Implement

		int[] num_column = {1};

		while(store.getColumns(num_column)[0].eof!=true)
		{
			num_column[0] = num_column[0]+1;

		}
		num_column[0] = num_column[0]-1;

		int[] column =new int[num_column[0]];
		for(int i=0;i<num_column[0];i++)
		{
			column[i] = i+1;
		}
		return (store.getColumns(column));


	}

}
