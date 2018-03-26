package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class HashJoin implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator leftChild;
	VolcanoOperator rightChid;
	int leftFieldNo;
	int rightFieldNo;
	ArrayList<DBTuple> buff;
	int pointer;
	ArrayList<DBTuple> buffleft;

	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
		this.leftChild = leftChild;
		this.rightChid = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		buff = new ArrayList<DBTuple>();
		buffleft = new ArrayList<DBTuple>();
		leftChild.open();
		rightChid.open();
		pointer =0;

	}

	@Override
	public DBTuple next()
    {
		// TODO: Implement
		try
		{
			if(pointer==0)
			{
				DBTuple templeft = leftChild.next();
				DBTuple tempright = rightChid.next();

				while (templeft!=null)
				{
					buffleft.add(templeft);
					templeft=leftChild.next();
				}

				for(DBTuple Left:buffleft)
				{

					if(Left.fields[leftFieldNo]==tempright.fields[rightFieldNo])
					{
						Object[] combine= combineTwoArrays(Left.fields,tempright.fields);
						DataType[] combine_type = combineTwoArrays(Left.types,tempright.types);
						DBTuple Combine =new DBTuple(combine,combine_type);
						buff.add(Combine);
					}

				}
			}

			pointer++;
			if(pointer-1>=buff.size()) {
				DBTuple tempright = rightChid.next();
				for(DBTuple Left:buffleft)
				{
					if(Left.fields[leftFieldNo]==tempright.fields[rightFieldNo])
					{
						Object[] combine= combineTwoArrays(Left.fields,tempright.fields);
						DataType[] combine_type = combineTwoArrays(Left.types,tempright.types);
						DBTuple Combine =new DBTuple(combine,combine_type);
						buff.add(Combine);
					}

				}
			}return buff.get(pointer-1);
		}catch (Exception e)
		{
			close();
			DBTuple rtn = new DBTuple();
			return rtn;

		}
	}

	@Override
	public void close() {
		// TODO: Implement
        buff=null;
        buffleft=null;
        pointer = 0;
        leftChild.close();
        rightChid.close();
	}

	public static <T> T[] combineTwoArrays(T[] first,T[] second)
	{
		T[] result = Arrays.copyOf(first,first.length+second.length);
		System.arraycopy(second,0,result,first.length,second.length);
		return result;
	}
}
