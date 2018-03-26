package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import org.omg.PortableInterceptor.INACTIVE;

import java.util.*;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	// TODO: Add required structures
	VectorOperator leftChild;
	VectorOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;
	ArrayList<DBColumn[]> buff;
	ArrayList<DBColumn[]> buffleft;
	int pointer;
	int vectorsize;



	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		pointer = 0;
		buff = new ArrayList<DBColumn[]>();
		buffleft = new ArrayList<DBColumn[]>();
		leftChild.open();
		rightChild.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
		try {
			DBColumn[] temp_left = leftChild.next();
			DBColumn[] temp_right = rightChild.next();
			if (pointer == 0) {
				vectorsize = Math.max(temp_left[0].attributes.length, temp_right[0].attributes.length);
				DBColumn[] combine = new DBColumn[temp_left.length];
				Object[] Start = {};
				for (int i = 0; i < temp_left.length; i++) {
					DataType type = temp_left[i].type;
					combine[i] = new DBColumn(Start, type);
				}//initialize combine ;

				//store data into buff
				while (temp_left != null) {
					int len = combine[0].attributes.length;
					for (int column = 0; column < temp_left.length; column++) {

						//creat attributes for combine DBColumns.attribute
						Object[] Attributes = new Object[temp_left[column].attributes.length + len];
						for (int i = 0; i < Attributes.length; i++) {
							if (i < len) {
								Attributes[i] = combine[column].attributes[i];
							} else {
								Attributes[i] = temp_left[column].attributes[i - len];
							}
						}
						DataType type = temp_left[column].type;
						combine[column] = new DBColumn(Attributes, type);
					}

					temp_left = leftChild.next();
				}
				buffleft.add(combine);

				DBColumn[] Selected = Combine(combine, temp_right);

				pointer = pointer + 1;
				int Rownumber = Selected[0].attributes.length;

				int startindex = (pointer - 1) * vectorsize;
				int endindex = pointer * vectorsize;

				while (startindex < Rownumber) {
					if (endindex > Rownumber) {
						endindex = Rownumber;
					}

					DBColumn[] vector_return = new DBColumn[Selected.length];


					for (int i = 0; i < Selected.length; i++) {

						Object[] tuples = Arrays.copyOfRange(Selected[i].attributes, startindex, endindex);
						DataType type = Selected[i].type;
						vector_return[i] = new DBColumn(tuples, type);
					}

					buff.add(vector_return);
					startindex = startindex + vectorsize;
					endindex = endindex + vectorsize;
				}
			}else{
				pointer = pointer + 1;
			}

			DBColumn[] combine = buffleft.get(0);
			if (pointer - 1 >= buff.size()) {
				pointer=0;
				buff = new ArrayList<DBColumn[]>();
				//temp_right = rightChild.next();
				DBColumn[] Selected = Combine(combine, temp_right);
				int Rownumber = Selected[0].attributes.length;
				pointer++;
				int startindex = (pointer - 1) * vectorsize;
				int endindex = pointer * vectorsize;

				while (startindex < Rownumber) {
					if (endindex > Rownumber) {
						endindex = Rownumber;
					}

					DBColumn[] vector_return = new DBColumn[Selected.length];

					for (int i = 0; i < Selected.length; i++) {

						Object[] tuples = Arrays.copyOfRange(Selected[i].attributes, startindex, endindex);
						DataType type = Selected[i].type;
						vector_return[i] = new DBColumn(tuples, type);
					}

					buff.add(vector_return);
					startindex = startindex + vectorsize;
					endindex = endindex + vectorsize;
				}
			}
			return buff.get(pointer-1);
		}catch (Exception e) {
			close();
			return null;
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		leftChild.close();
		rightChild.close();
		buff = null;
		pointer =0;
	}


	public DBColumn[] Combine(DBColumn[] combine,DBColumn[] temp_right) {

		DBColumn[] Return = new DBColumn[combine.length+temp_right.length];
		ArrayList<Integer> right_index = new ArrayList<Integer>();
		ArrayList<Integer> combine_index = new ArrayList<Integer>();

		for(int row_right=0;row_right<temp_right[rightFieldNo].attributes.length;row_right++){
			for(int row_combine=0;row_combine<combine[leftFieldNo].attributes.length;row_combine++){
				if(temp_right[rightFieldNo].attributes[row_right]==combine[leftFieldNo].attributes[row_combine]){
					right_index.add(row_right);
					combine_index.add(row_combine);
				}
			}
		}

		for(int column=0;column<Return.length;column++)
		{
			if(column<combine.length){

				Object[] Attributes = new Object[combine_index.size()];
				for (int index = 0; index < combine_index.size(); index++)
				{
					Attributes[index] = combine[column].attributes[combine_index.get(index)];
				}

				DataType type = combine[column].type;
				Return[column] = new DBColumn(Attributes,type);
			}
			else{

				Object[] Attributes = new Object[right_index.size()];
				for (int index = 0; index < right_index.size(); index++)
				{
					Attributes[index] = temp_right[column-combine.length].attributes[right_index.get(index)];
				}

				DataType type = temp_right[column-combine.length].type;
				Return[column] = new DBColumn(Attributes,type);
			}

		}
		right_index =null;
		combine_index=null;
		return Return;

	}


}
