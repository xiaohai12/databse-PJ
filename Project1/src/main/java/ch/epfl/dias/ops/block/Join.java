package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import javax.jws.Oneway;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements BlockOperator {

	// TODO: Add required structures
	BlockOperator leftChild;
	BlockOperator rightChild;
	int leftFieldNo;
	int rightFieldNo;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;

	}

	public DBColumn[] execute() {
		// TODO: Implement

		DBColumn[] temp_left = leftChild.execute();
		DBColumn[] temp_right = rightChild.execute();
		try
		{
			DBColumn[] Return = new DBColumn[temp_left.length+temp_right.length];
			DBColumn co_left = temp_left[leftFieldNo];
			DBColumn co_right = temp_right[rightFieldNo];
			ArrayList<Integer> Index_left = new ArrayList<Integer>();
			ArrayList<Integer> Index_right = new ArrayList<Integer>();

			for (int left_index = 0; left_index < co_left.attributes.length; left_index++)
			{
				for (int right_index = 0; right_index < co_right.attributes.length; right_index++) {
					if (co_left.attributes[left_index] == co_right.attributes[right_index]) {
						Index_left.add(left_index);
						Index_right.add(right_index);

					}
				}
			}

			for(int column=0;column<Return.length;column++)
			{
				if(column<temp_left.length){

					Object[] Attributes = new Object[Index_left.size()];
					for (int index = 0; index < Index_left.size(); index++)
					{
						Attributes[index] = temp_left[column].attributes[Index_left.get(index)];
					}

					DataType type = temp_left[column].type;
					Return[column] = new DBColumn(Attributes,type);
				}
				else{

					Object[] Attributes = new Object[Index_right.size()];
					for (int index = 0; index < Index_right.size(); index++)
					{
						Attributes[index] = temp_right[column-temp_left.length].attributes[Index_right.get(index)];
					}

					DataType type = temp_right[column-temp_left.length].type;
					Return[column] = new DBColumn(Attributes,type);
				}
			}
			return Return;
		}catch (Exception e){
			return null;
		}
	}

}
