package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.util.jar.Attributes;

public class ProjectAggregate implements VectorOperator {

	// TODO: Add required structures
	VectorOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child =child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;

	}

	@Override
	public void open() {
		// TODO: Implement
		child.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		DBColumn[] temp = child.next();

		DBColumn[] Return = new DBColumn[1];

		try {
			DBColumn[] combine = new DBColumn[temp.length];

			Object[] Start = {};
			for (int i = 0; i < temp.length; i++) {
				DataType type = temp[i].type;
				combine[i] = new DBColumn(Start, type);
			}//initialize combine ;

			//store data into combine
			while (temp != null) {
				int len = combine[0].attributes.length;
				for (int column = 0; column < temp.length; column++) {

					//creat attributes for combine DBColumns.attribute
					Object[] Attributes = new Object[temp[column].attributes.length + len];
					for (int i = 0; i < Attributes.length; i++) {
						if (i < len) {
							Attributes[i] = combine[column].attributes[i];
						} else {
							Attributes[i] = temp[column].attributes[i - len];
						}
					}
					DataType type = temp[column].type;
					combine[column] = new DBColumn(Attributes, type);
				}

				temp = child.next();
			}


			DBColumn Attributes = combine[fieldNo];

			if (agg.name() == "COUNT") {
				Object[] count = {Attributes.attributes.length};
				Return[0] = new DBColumn(count, dt);
			} else if (agg.name() == "SUM") {
				Object[] sum = new Object[1];
				if (dt.name() == "DOUBLE") {
					double Sum = 0.0;
					for (int i = 0; i < Attributes.attributes.length; i++) {
						Sum += (int) Attributes.attributes[i];
					}
					sum[0] = Sum;
				} else {
					int Sum = 0;
					for (int i = 0; i < Attributes.attributes.length; i++) {
						Sum += (int) Attributes.attributes[i];
					}
					sum[0] = Sum;
				}
				Return[0] = new DBColumn(sum, dt);
			} else if (agg.name() == "MAX") {
				Object[] max = new Object[1];
				if (dt.name() == "DOUBLE") {
					double Max = (int) Attributes.attributes[0];
					for (int i = 0; i < Attributes.attributes.length; i++) {
						if (Max < (int) Attributes.attributes[i]) {
							Max = (int) Attributes.attributes[i];
						}
					}
					max[0] = Max;
				} else {
					int Max = (int) Attributes.attributes[0];
					for (int i = 0; i < Attributes.attributes.length; i++) {
						if (Max < (int) Attributes.attributes[i]) {
							Max = (int) Attributes.attributes[i];
						}
					}
					max[0] = Max;
				}
				Return[0] = new DBColumn(max, dt);
			} else if (agg.name() == "MIN") {
				Object[] min = new Object[1];
				if (dt.name() == "DOUBLE") {
					double Min = (int) Attributes.attributes[0];
					for (int i = 0; i < Attributes.attributes.length; i++) {
						if (Min < (int) Attributes.attributes[i]) {
							Min = (int) Attributes.attributes[i];
						}
					}
					min[0] = Min;
				} else {
					int Min = (int) Attributes.attributes[0];
					for (int i = 0; i < Attributes.attributes.length; i++) {
						if (Min > (int) Attributes.attributes[i]) {
							Min = (int) Attributes.attributes[i];
						}
					}
					min[0] = Min;
				}
				Return[0] = new DBColumn(min, dt);
			} else if (agg.name() == "AVG") {
				Object[] avg = new Object[1];

				double Sum = 0.0;
				for (int i = 0; i < Attributes.attributes.length; i++) {
					Sum += (int) Attributes.attributes[i];
				}
				avg[0] = Sum / Attributes.attributes.length;
				Return[0] = new DBColumn(avg, dt);
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
