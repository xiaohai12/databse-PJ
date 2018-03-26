package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	// TODO: Implement
	public Object[] attributes;
	public DataType type;
	public boolean eof;

	public DBColumn(Object[] attributes, DataType type) {
		this.attributes = attributes;
		this.type = type;
		this.eof = false;

		// this is a constructor used to creat instance with parameter.
	}

	public DBColumn() {
		this.eof = true;
	}


	public Integer[] getAsInteger() {
		// TODO: Implement
		Integer[] Return =new Integer[attributes.length];
		for(int row=0;row<attributes.length;row++){
			Return[row] = (int) attributes[row];
		}

			return Return;



	}
}
