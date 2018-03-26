package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;

public class Select implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator child;
	BinaryOp op;
	int fieldNo;
	int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {

		// TODO: Implement
		child.open();


	}

	@Override
	public DBTuple next() {
		// TODO: Implement
		DBTuple rtn = null;

		try {

			while (rtn == null) {

				DBTuple temp = child.next();
				if (op.name() == "EQ") {
					if ((int) temp.fields[fieldNo] == value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}

				} else if (op.name() == "LT") {
					if ((int) temp.fields[fieldNo] < value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}
				} else if (op.name() == "LE") {
					if ((int) temp.fields[fieldNo] <= value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}

				} else if (op.name() == "NE") {
					if ((int) temp.fields[fieldNo] != value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}

				} else if (op.name() == "GT") {
					if ((int) temp.fields[fieldNo] > value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}
				} else if (op.name() == "GE") {

					if ((int) temp.fields[fieldNo] >= value) {
						rtn = new DBTuple(temp.fields, temp.types);
					}
				}
			}return rtn;
		} catch (Exception e){
			close();
			rtn = new DBTuple();
			return rtn;
		}
	}
	@Override
	public void close() {
		// TODO: Implement
		child.close();

	}
}
