package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class Scan implements VolcanoOperator {

	// TODO: Add required structures

    int point;
    boolean eof;
    ArrayList<DBTuple> buff;
    Store store;


	public Scan(Store store) {

		// TODO: Implement
		this.store = store;

	}

	@Override
	public void open() {
		// TODO: Implement
		buff = new ArrayList<DBTuple>();
		point = 0;



	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		try{
			eof = store.getRow(point+1).eof;
			if(eof ==false)
			{
				point = point + 1;
				buff.add(store.getRow(point));

				return buff.get(point-1);
			}
			else
			{
				close();
				return null;

			}

		}catch (Exception e){
			return null;
		}



	}

	@Override
	public void close() {
		// TODO: Implement
		buff = null;
		point = 0;


	}
}