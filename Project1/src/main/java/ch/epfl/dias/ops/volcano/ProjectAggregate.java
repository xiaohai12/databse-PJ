package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;
import jdk.nashorn.internal.ir.ReturnNode;

import java.util.ArrayList;
import java.util.stream.IntStream;

public class ProjectAggregate implements VolcanoOperator {

	// TODO: Add required structures
	VolcanoOperator child;
	Aggregate agg;
	DataType dt;
	int fieldNo;
	ArrayList<DBTuple> buff;
	int pointer;


	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
		// TODO: Implement
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
		buff = new ArrayList<DBTuple>();
		pointer =0;
		child.open();

	}

	@Override
	public DBTuple next()
    {
		// TODO: Implement
       Object[] record = new Object[1];
       DataType[] DT=new DataType[1];
       DT[0] = dt;
       DBTuple Return = new DBTuple();
       DBTuple temp = child.next();
       try{
           if(pointer==0){
               if (temp!=null)
               {
                   while (temp.eof!=true)
                   {

                       buff.add(temp);

                       temp = child.next();
                   }

                   if(dt.name()=="INT")
                   {

                       if(agg.name() =="COUNT")
                       {
                           int Record;
                           Record = buff.size();
                           record[0]=Record;
                           Return = new DBTuple(record,DT);

                       }

                       else if(agg.name()=="SUM")
                       {
                           int Record = 0;
                           for(DBTuple i : buff)
                           {
                               Record += (int)i.fields[fieldNo];
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);
                       }

                       else if(agg.name()=="MAX")
                       {
                           int Record=(int)buff.get(0).fields[fieldNo];
                           for(DBTuple i : buff)
                           {
                               if((int)i.fields[fieldNo]>Record)
                               {
                                   Record =(int) i.fields[fieldNo];
                               }
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);


                       }

                       else if(agg.name()=="MIN")
                       {
                           int Record=(int)buff.get(0).fields[fieldNo];
                           for(DBTuple i : buff)
                           {
                               if((int)i.fields[fieldNo]<Record)
                               {
                                   Record =(int) i.fields[fieldNo];
                               }
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);

                       }

                       else if(agg.name()=="AVG")
                       {
                           int Record = 0;
                           for(DBTuple i : buff)
                           {
                               Record += (int)i.fields[fieldNo];
                           }
                           record[0] = (Record/buff.size());
                           Return = new DBTuple(record,DT);

                       }
                   }else if (dt.name()=="DOUBLE")
                   {


                       if(agg.name() =="COUNT")
                       {
                           double Record;
                           Record = buff.size();
                           record[0]=Record;
                           Return = new DBTuple(record,DT);

                       }

                       else if(agg.name()=="SUM")
                       {
                           double Record=0.0;
                           for(DBTuple i : buff)
                           {
                               Record += (double)i.fields[fieldNo];
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);


                       }

                       else if(agg.name()=="MAX")
                       {
                           double Record=(double)buff.get(0).fields[fieldNo];
                           for(DBTuple i : buff)
                           {
                               if((double)i.fields[fieldNo]>Record)
                               {
                                   Record =(double) i.fields[fieldNo];
                               }
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);


                       }

                       else if(agg.name()=="MIN")
                       {
                           double Record=(double)buff.get(0).fields[fieldNo];
                           for(DBTuple i : buff)
                           {
                               if((double)i.fields[fieldNo]<Record)
                               {
                                   Record =(double) i.fields[fieldNo];
                               }
                           }
                           record[0] = Record;
                           Return = new DBTuple(record,DT);

                       }

                       else if(agg.name()=="AVG")
                       {
                           double Record = 0.0;
                           for (DBTuple i : buff)
                           {
                               Record += (double) i.fields[fieldNo];
                           }
                           record[0] = (Record / buff.size());
                           Return = new DBTuple(record, DT);
                       }
                   }
                   pointer ++;
                   return Return;
               }else{
                   return null;
               }

           }else{

               return null;
           }

       }catch (Exception e){
           close();
           return null;
       }


    }

	@Override
	public void close() {
		// TODO: Implement
        buff = null;
        pointer=0;
        child.close();
	}

}
