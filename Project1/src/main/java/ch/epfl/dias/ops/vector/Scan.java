package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Scan implements VectorOperator {

	// TODO: Add required structures
	Store store;
	int vectosize;
	int pointer;
	ArrayList<DBColumn[]> buff;

	public Scan(Store store, int vectorsize) {
		// TODO: Implement
		this.store= store;
		this.vectosize = vectorsize;
	}
	
	@Override
	public void open() {
		// TODO: Implement
		pointer=0;
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		try
		{
			int [] ColumnNum= {1};
			int Rownumber = store.getColumns(ColumnNum)[0].attributes.length;

			while(store.getColumns(ColumnNum)[0].eof!=true)
			{
				ColumnNum[0] = ColumnNum[0]+1;
			}

			ColumnNum[0] = ColumnNum[0]-1;

			int[] colums = new int[ColumnNum[0]];
			for(int i=0;i<ColumnNum[0];i++)
			{
				colums[i] = i+1;
			}

			DBColumn[] temp = store.getColumns(colums);
			//obtain the total data;

			pointer = pointer+1;

			int startindex = (pointer-1)*vectosize;
			int endindex = pointer*vectosize;
			while(startindex<Rownumber)
			{
				if(endindex>Rownumber)
				{
					endindex = Rownumber;
				}

				DBColumn[] Return = new DBColumn[temp.length];

				for(int i=0;i<ColumnNum[0];i++)
				{

					Object[] tuples = Arrays.copyOfRange(temp[i].attributes,startindex,endindex);
					DataType type = temp[i].type;
					Return[i] = new DBColumn(tuples,type);
				}
				return Return;
			}return null;


		}catch (Exception e){
			close();
			return null;
		}


	}

	@Override
	public void close() {

		// TODO: Implement
		pointer = 0;
		buff=null;
	}
}
