package ch.epfl.dias.ops.block;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements BlockOperator {

	// TODO: Add required structures
	BlockOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNo;
	
	public ProjectAggregate(BlockOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute()
	{
		// TODO: Implement
		DBColumn[] temp = child.execute();

		try{
			DBColumn Attributes = temp[fieldNo];
			DBColumn[] Return = new DBColumn[1];
			Object[] re_obj = new Object[1];
			if(agg.name()=="COUNT")
			{
				if(dt.name()=="DOUBLE")
				{
					double count =  Attributes.attributes.length;
					re_obj[0] = count;
				}
				else{
					re_obj[0] = Attributes.attributes.length;
				}
				Return[0] = new DBColumn(re_obj,dt);
			}

			else if(agg.name()=="SUM")
			{
				if(dt.name()=="DOUBLE")
				{
					double sum=0.0;
					for(int row=0;row<Attributes.attributes.length;row++)
					{

						sum +=(int)Attributes.attributes[row];
					}
					re_obj[0]= sum;

				}else
				{
					int sum=0;
					for(int row=0;row<Attributes.attributes.length;row++)
					{
						sum +=(int) Attributes.attributes[row];
					}
					re_obj[0]= sum;
				}
				Return[0] = new DBColumn(re_obj,dt);

			}

			else if(agg.name()=="MAX")
			{
				if (dt.name() == "DOUBLE")
				{
					double max = (int)Attributes.attributes[0];
					for (int row = 0; row < Attributes.attributes.length; row++)
					{
						if ((int) Attributes.attributes[row] > max)
						{
							max = (int) Attributes.attributes[row];
						}
					}

					re_obj[0] = max;

				} else {
					int max = (int) Attributes.attributes[0];
					for (int row = 0; row < Attributes.attributes.length; row++)
					{
						if ((int) Attributes.attributes[row] > max)
						{
							max = (int) Attributes.attributes[row];
						}
					}
					re_obj[0] = max;
				}
				Return[0] = new DBColumn(re_obj, dt);
			}

			else if(agg.name()=="MIN")
			{
				if (dt.name() == "DOUBLE")
				{
					double min = (int) Attributes.attributes[0];
					for (int row = 0; row < Attributes.attributes.length; row++)
					{
						if ((int) Attributes.attributes[row] < min)
						{
							min = (int) Attributes.attributes[row];
						}
					}

					re_obj[0] = min;

				} else {
					int min = (int)Attributes.attributes[0];
					for (int row = 0; row < Attributes.attributes.length; row++)
					{
						if ((int) Attributes.attributes[row] < min)
						{
							min = (int) Attributes.attributes[row];
						}
					}
					re_obj[0] = min;
				}
				Return[0] = new DBColumn(re_obj, dt);
			}

			else if(agg.name()=="AVG")
			{
				double sum=0.0;

				for(int row=0;row<Attributes.attributes.length;row++)
				{
					sum +=(int) Attributes.attributes[row];
				}
				re_obj[0]= sum;
				re_obj[0]= sum/Attributes.attributes.length;
				Return[0] = new DBColumn(re_obj,dt);
			}
			return Return;

		}catch (Exception e)
		{
			return null;
		}

	}

}
