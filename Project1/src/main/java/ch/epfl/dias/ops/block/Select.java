package ch.epfl.dias.ops.block;

import java.util.ArrayList;
import java.util.jar.Attributes;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import com.sun.nio.sctp.PeerAddressChangeNotification;
import org.w3c.dom.Attr;

public class Select implements BlockOperator {

	// TODO: Add required structures
	BlockOperator child;
	BinaryOp op;
	int fielNo;
	int value;

	public Select(BlockOperator child, BinaryOp op, int fieldNo, int value) {
		// TODO: Implement
		this.child = child;
		this.op = op;
		this.fielNo=fieldNo;
		this.value = value;
	}

	@Override
	public DBColumn[] execute()
	{
		// TODO: Implement
		DBColumn[] temp = child.execute();
		try
		{
			ArrayList<Integer> index= new ArrayList<Integer>();
			for(int i=0;i<temp[fielNo].attributes.length;i++)
			{
				if(op.name()=="EQ")
				{
					if((int)temp[fielNo].attributes[i]==value)
					{
						index.add(i);

					}
				}else if(op.name()=="LT")
				{
					if((int)temp[fielNo].attributes[i]<value)
					{
						index.add(i);

					}
				}else if(op.name()=="LE")
				{
					if((int)temp[fielNo].attributes[i]<=value)
					{
						index.add(i);

					}
				}else if(op.name()=="GT")
				{
					if((int)temp[fielNo].attributes[i]>value)
					{
						index.add(i);

					}
				}else if(op.name()=="GE")
				{
					if((int)temp[fielNo].attributes[i]>=value)
					{
						index.add(i);

					}
				}else if(op.name()=="NE")
				{
					if((int)temp[fielNo].attributes[i]!=value)
					{
						index.add(i);

					}
				}

			}
			if(index.size()==0)
			{
				return null;
			}
			else{
				DBColumn[] Return= new DBColumn[temp.length];

				for(int column=0;column<temp.length;column++)
				{
					Object[] Attri = new Object[index.size()];
					for(int row=0;row<index.size();row++)
					{
						Attri[row] = temp[column].attributes[index.get(row)];
						DataType type = temp[column].type;
						Return[column] = new DBColumn(Attri,type);
					}
				}
				return  Return;
			}


		}catch (Exception e)
		{
			return null;
		}
	}
}
