package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import com.sun.corba.se.impl.ior.OldJIDLObjectKeyTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.jar.Attributes;

public class Select implements VectorOperator {

	// TODO: Add required structures
	VectorOperator child;
	BinaryOp op;
	int fieldNo;
	int value;
	int vectorsize;
	int pointer;
	ArrayList<DBColumn[]> Buff;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open()
	{
		// TODO: Implement
		child.open();
		Buff = new ArrayList<DBColumn[]>();
		pointer=0;
	}

	@Override
	public DBColumn[] next()
	{
		// TODO: Implement
		DBColumn[] temp = child.next();
		try{
			if(pointer==0) {
				vectorsize = temp[0].attributes.length;
				DBColumn[] combine = new DBColumn[temp.length];
				DBColumn[] Selected = new DBColumn[temp.length];
				Object[] Start = {};
				for (int i = 0; i < temp.length; i++) {
					DataType type = temp[i].type;
					combine[i] = new DBColumn(Start, type);
				}//initialize combine ;

				//store data into buff
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

				//select index we want.and then create new DBColumn list
				Object[] Attribute_combine = combine[fieldNo].attributes;
				ArrayList<Integer> Indexes = option(op, Attribute_combine);


				for (int column = 0; column < combine.length; column++) {
					Object[] fieldValue = new Object[Indexes.size()];
					for (int tuple = 0; tuple < fieldValue.length; tuple++) {
						fieldValue[tuple] = combine[column].attributes[Indexes.get(tuple)];
					}
					DataType type = combine[column].type;
					Selected[column] = new DBColumn(fieldValue, type);
				}

				//divide Return into vector part and store in buff

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

					Buff.add(vector_return);
					startindex = startindex + vectorsize;
  				    endindex = endindex + vectorsize;
				}
				return Buff.get(pointer - 1);
			}else
			{
				pointer = pointer+1;
				return Buff.get(pointer - 1);

			}

		}catch(Exception e)
		{
			close();
			return null;
		}
	}





	@Override
	public void close() {
		// TODO: Implement
		child.close();
		pointer=0;
		Buff=null;
	}

	public ArrayList<Integer> option(BinaryOp op,Object[] Attributes)
	{

		ArrayList<Integer> Index = new ArrayList<Integer>();

		if(op.name()=="EQ")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] == value)
				{
					Index.add(index);
				}
			}
		}else if(op.name()=="LT")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] < value)
				{
					Index.add(index);
				}
			}
		}else if(op.name()=="LQ")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] <= value)
				{
					Index.add(index);
				}
			}
		}else if(op.name()=="GT")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] > value)
				{
					Index.add(index);
				}
			}
		}else if(op.name()=="GE")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] >= value)
				{
					Index.add(index);
				}
			}
		}else if(op.name()=="NQ")
		{
			for (int index = 0; index < Attributes.length; index++)
			{
				if ((int) Attributes[index] != value)
				{
					Index.add(index);
				}
			}
		}
		return Index;

	}

}
