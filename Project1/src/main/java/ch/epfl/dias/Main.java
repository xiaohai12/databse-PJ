package ch.epfl.dias;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
//import ch.epfl.dias.ops.vector.Project;
import ch.epfl.dias.ops.volcano.Project;
import ch.epfl.dias.ops.vector.Scan;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.ops.volcano.ProjectAggregate;
import ch.epfl.dias.ops.volcano.Select;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.RowStore;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.row.DBTuple;

import java.util.ArrayList;
import java.util.Arrays;

public class Main
{

	public static void main(String[] args)
	{


		int[] a = new int[]{1,2,3,4,5,6,7};
		int[] b = Arrays.copyOfRange(a,2,5);
		System.out.println(b[1]);

		DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[]lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };


//		RowStore rowstoreData = new RowStore(schema, "input/data.csv", ",");
//		rowstoreData.load();
//
//		RowStore rowstoreOrder = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
//		rowstoreOrder.load();
//
//
//
//		PAXStore paxstore = new PAXStore(lineitemSchema, "input/lineitem_small.csv", "\\|", 3);
//		paxstore.load();


		////----------------------------------///
		///this part is used to answer the question 3

		//1.Compare with PAX AND NSM
		//----------------------------------//
		//1.1

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			RowStore rowstoreLineItem = new RowStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			rowstoreLineItem.load();
//			ArrayList<DBTuple>  row_data = new ArrayList<DBTuple>();
//			ch.epfl.dias.ops.volcano.Scan scan_ROW= new ch.epfl.dias.ops.volcano.Scan(rowstoreLineItem);
//			ch.epfl.dias.ops.volcano.Select sel1_row = new ch.epfl.dias.ops.volcano.Select(scan_ROW, BinaryOp.EQ, 0, 1);
//			ch.epfl.dias.ops.volcano.Select sel2_row = new ch.epfl.dias.ops.volcano.Select(sel1_row, BinaryOp.GT, 3, 3);
//			int[] columns = {0,4};
//			ch.epfl.dias.ops.volcano.Project pro_Row = new ch.epfl.dias.ops.volcano.Project(sel2_row,columns);
//			pro_Row.open();
//			DBTuple tmp = pro_Row.next();
//			while(tmp!=null){
//				row_data.add(tmp);
//				tmp = pro_Row.next();
//			}
//			i++;
//		}
//
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");

		int i =0;
		long startTime=System.currentTimeMillis();
		while(i<5){
			PAXStore paxstore = new PAXStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|", 1000);
			paxstore.load();
			ArrayList<DBTuple>  PAX_data = new ArrayList<DBTuple>();
			ch.epfl.dias.ops.volcano.Scan scan_ROW= new ch.epfl.dias.ops.volcano.Scan(paxstore);
			ch.epfl.dias.ops.volcano.Select sel1_row = new ch.epfl.dias.ops.volcano.Select(scan_ROW, BinaryOp.EQ, 0, 1);
			ch.epfl.dias.ops.volcano.Select sel2_row = new ch.epfl.dias.ops.volcano.Select(sel1_row, BinaryOp.GT, 3, 3);
			int[] columns = {0,4};
			ch.epfl.dias.ops.volcano.Project pro_Row = new ch.epfl.dias.ops.volcano.Project(sel2_row,columns);
			pro_Row.open();
			DBTuple tmp = pro_Row.next();
			while(tmp!=null){
				PAX_data.add(tmp);
				tmp = pro_Row.next();
			}
			i++;
		}
		long endTime=System.currentTimeMillis();
		long Time = (endTime-startTime)/5;
		System.out.println("time： "+Time+"ms");


		//-------------------------------------------------------//
		//1.2

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			PAXStore paxstore_lin = new PAXStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|", 1000);
//			paxstore_lin.load();
//			PAXStore paxstore_ord = new PAXStore(orderSchema, "input/big/orders_big.csv", "\\|", 1000);
//			paxstore_ord.load();
//			ArrayList<DBTuple>  PAX_data = new ArrayList<DBTuple>();
//			ch.epfl.dias.ops.volcano.Scan scan_Lin= new ch.epfl.dias.ops.volcano.Scan(paxstore_lin);
//			ch.epfl.dias.ops.volcano.Scan scan_Ord= new ch.epfl.dias.ops.volcano.Scan(paxstore_ord);
//			ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan_Lin,scan_Ord,0,0);
//			int[] clo={10,20};
//			ch.epfl.dias.ops.volcano.Project projects = new ch.epfl.dias.ops.volcano.Project(join,clo);
//			projects.open();
//			DBTuple tmp = projects.next();
//			while(tmp!=null){
//				PAX_data.add(tmp);
//				tmp = projects.next();
//			}
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");

//
//		int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//			RowStore rowstore_lin = new RowStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			rowstore_lin.load();
//			RowStore rowstore_ord = new RowStore(orderSchema, "input/big/orders_big.csv", "\\|");
//			rowstore_ord.load();
//			ArrayList<DBTuple>  Row_data = new ArrayList<DBTuple>();
//			ch.epfl.dias.ops.volcano.Scan scan_Lin= new ch.epfl.dias.ops.volcano.Scan(rowstore_lin);
//			ch.epfl.dias.ops.volcano.Scan scan_Ord= new ch.epfl.dias.ops.volcano.Scan(rowstore_ord);
//			ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan_Lin,scan_Ord,0,0);
//			int[] clo={10,20};
//			ch.epfl.dias.ops.volcano.Project projects = new ch.epfl.dias.ops.volcano.Project(join,clo);
//			projects.open();
//			DBTuple tmp = projects.next();
//			while(tmp!=null){
//				Row_data.add(tmp);
//				tmp = projects.next();
//			}
		//	i++;
		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");


//      //---------------------------------//
		//1.3

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			RowStore rowstore_lin = new RowStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			rowstore_lin.load();
//			ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore_lin);
//			ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 3, 4);
//			ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 3);
//			agg.open();
//			DBTuple tmp = agg.next();
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");



//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			PAXStore paxstore_lin = new PAXStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|", 1000);
//			paxstore_lin.load();
//			ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(paxstore_lin);
//			ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 3, 4);
//			ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 3);
//			agg.open();
//			DBTuple tmp = agg.next();
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");


		//---------------------------------//
		//1.4
//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			PAXStore paxstore_lin = new PAXStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|", 1000);
//			paxstore_lin.load();
//			PAXStore paxstore_ord = new PAXStore(orderSchema, "input/big/orders_big.csv", "\\|", 1000);
//			paxstore_ord.load();
//			ch.epfl.dias.ops.volcano.Scan scan_Lin= new ch.epfl.dias.ops.volcano.Scan(paxstore_lin);
//			ch.epfl.dias.ops.volcano.Scan scan_Ord= new ch.epfl.dias.ops.volcano.Scan(paxstore_ord);
//			ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan_Lin,scan_Ord,0,0);
//			ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 19);
//			agg.open();
//			DBTuple tmp = agg.next();
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			RowStore rowstore_lin = new RowStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			rowstore_lin.load();
//			RowStore rowstore_ord = new RowStore(orderSchema, "input/big/orders_big.csv", "\\|");
//			rowstore_ord.load();
//			ch.epfl.dias.ops.volcano.Scan scan_Lin= new ch.epfl.dias.ops.volcano.Scan(rowstore_lin);
//			ch.epfl.dias.ops.volcano.Scan scan_Ord= new ch.epfl.dias.ops.volcano.Scan(rowstore_ord);
//			ch.epfl.dias.ops.volcano.HashJoin join = new ch.epfl.dias.ops.volcano.HashJoin(scan_Lin,scan_Ord,0,0);
//			ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 19);
//			agg.open();
//			DBTuple tmp = agg.next();
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");



		//2.Compare with Column-at-a-time and Vector-at-a-time
		//----------------------------------//
		//2.1

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//			ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//			ch.epfl.dias.ops.block.Scan scan_ROW= new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
//			ch.epfl.dias.ops.block.Select sel1_row = new ch.epfl.dias.ops.block.Select(scan_ROW, BinaryOp.EQ, 0, 1);
//			ch.epfl.dias.ops.block.Select sel2_row = new ch.epfl.dias.ops.block.Select(sel1_row, BinaryOp.GT, 3, 3);
//			int[] columns = {0,4};
//			ch.epfl.dias.ops.block.Project pro_Row = new ch.epfl.dias.ops.block.Project(sel2_row,columns);
//			DBColumn[] result = pro_Row.execute();
//			i++;
//		}
//
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");



//		int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//			ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//			ch.epfl.dias.ops.vector.Scan scan_ROW= new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem,500);
//			ch.epfl.dias.ops.vector.Select sel1_row = new ch.epfl.dias.ops.vector.Select(scan_ROW, BinaryOp.EQ, 0, 1);
//			ch.epfl.dias.ops.vector.Select sel2_row = new ch.epfl.dias.ops.vector.Select(sel1_row, BinaryOp.GT, 3, 2);
//			int[] columns = {0,4};
//			ch.epfl.dias.ops.vector.Project pro_Row = new ch.epfl.dias.ops.vector.Project(sel2_row,columns);
//			pro_Row.open();;
//			DBColumn[] result = pro_Row.next();
//			ArrayList<DBColumn[]>  Column_data = new ArrayList<DBColumn[]>();
//			while(result!=null)
//			{
//				Column_data.add(result);
//				result = pro_Row.next();
//			}
//		//i++;
//		//}
//
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime);
//		System.out.println("time： "+Time+"ms");





		//------------------------------//
		//2.2
//		int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//		ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//		columnstoreLineItem.load();
//		ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/big/orders_big.csv", "\\|");
//		columnstoreOrder.load();
//			ch.epfl.dias.ops.block.Scan scan_Lin= new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
//			ch.epfl.dias.ops.block.Scan scan_Ord= new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
//			ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scan_Lin,scan_Ord,0,0);
//			int[] clo={10,20};
//			ch.epfl.dias.ops.block.Project projects = new ch.epfl.dias.ops.block.Project(join,clo);
//			DBColumn[] tmp = projects.execute();
//			//i++;
//		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime);
//		System.out.println("time： "+Time+"ms");


//		int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//			ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//			ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/big/orders_big.csv", "\\|");
//			columnstoreOrder.load();
//
//			ch.epfl.dias.ops.vector.Scan scan_Lin= new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem,500);
//			ch.epfl.dias.ops.vector.Scan scan_Ord= new ch.epfl.dias.ops.vector.Scan(columnstoreOrder,500);
//			ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan_Lin,scan_Ord,0,0);
//			int[] clo={10,20};
//			ch.epfl.dias.ops.vector.Project projects = new ch.epfl.dias.ops.vector.Project(join,clo);
//			projects.open();
//			DBColumn[] tmp = projects.next();
//		    ArrayList<DBColumn[]>  Column_data = new ArrayList<DBColumn[]>();
//			while(tmp!=null){
//				Column_data.add(tmp);
//				tmp = projects.next();
//			}
//		//	i++;
//		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime);
//		System.out.println("time： "+Time+"ms");



		//-------------------------//
		//2.3

//		int i =0;
//		long startTime=System.currentTimeMillis();
//		while(i<10){
//		    ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//			ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
//			ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.GE, 3, 4);
//			ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 3);
//			DBColumn[] tmp = agg.execute();
//			i++;
//		}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");


//		//int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//			ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//			ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem,10000);
//			ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, 3, 4);
//			ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 3);
//			agg.open();
//			DBColumn[] tmp = agg.next();
//		//	i++;
//		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime);
//		System.out.println("time： "+Time+"ms");




		//-------------------------------//
		//2.4
		//		int i =0;
//		long startTime=System.currentTimeMillis();
//		//while(i<10){
//		    ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//			columnstoreLineItem.load();
//		    ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/big/orders_big.csv", "\\|");
//		    columnstoreOrder.load();
//
//			ch.epfl.dias.ops.block.Scan scan_Lin= new ch.epfl.dias.ops.block.Scan(columnstoreLineItem);
//			ch.epfl.dias.ops.block.Scan scan_Ord= new ch.epfl.dias.ops.block.Scan(columnstoreOrder);
//			ch.epfl.dias.ops.block.Join join = new ch.epfl.dias.ops.block.Join(scan_Lin,scan_Ord,0,0);
//			ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 19);
//			DBColumn[] tmp = agg.execute();
//			//i++;
//		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime)/10;
//		System.out.println("time： "+Time+"ms");

//
//	    int i =0;
//	    long startTime=System.currentTimeMillis();
//	//while(i<10){
//		ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/big/lineitem_big.csv", "\\|");
//		columnstoreLineItem.load();
//		ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/big/orders_big.csv", "\\|");
//		columnstoreOrder.load();
//
//		ch.epfl.dias.ops.vector.Scan scan_Lin= new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem,500);
//		ch.epfl.dias.ops.vector.Scan scan_Ord= new ch.epfl.dias.ops.vector.Scan(columnstoreOrder,500);
//		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scan_Lin,scan_Ord,0,0);
//		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 19);
//		agg.open();
//		DBColumn[] tmp = agg.next();
//		//i++;
//		//}
//		long endTime=System.currentTimeMillis();
//		long Time = (endTime-startTime);
//		System.out.println("time： "+Time+"ms");


		// This query should return only one result




//
//		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
//		rowstore.load();
//
//		//DBTuple c = rowstore.getRow(12);
		//PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
		//paxstore.load();
//		//DBTuple c = paxstore.getRow(3);
//
//		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
//		//scan.open();
//		DBTuple currentTuple = scan.next();
//		while (!currentTuple.eof)
//		{
//		System.out.println(currentTuple.getFieldAsInt(1));
//		currentTuple = scan.next();
//		 }
//
//		 ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
//		 columnstoreData.load();
//
//
//		ColumnStore columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
//		columnstoreOrder.load();
//
//		ColumnStore columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
//		columnstoreLineItem.load();
//
//		 ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder,3);
//		 ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem,3);
//		 ch.epfl.dias.ops.vector.Select selOrder = new ch.epfl.dias.ops.vector.Select(scanOrder,BinaryOp.EQ,0,1);
//		 ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem,BinaryOp.EQ,0,1);
//		 int[] pro = {0,1,3};
//		 ch.epfl.dias.ops.vector.Project prj = new ch.epfl.dias.ops.vector.Project(selLineitem,pro);
//		 prj.open();
//		 DBColumn[] x = prj.next();
//		DBColumn[] y = prj.next();
//		DBColumn[] T = prj.next();
//		 ch.epfl.dias.ops.vector.Select selLineitem = new ch.epfl.dias.ops.vector.Select(scanLineitem,BinaryOp.EQ,0,1);
//		 ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(selOrder, selLineitem, 0, 0);
//		 ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.DOUBLE, 0);
//     	 agg.open();
//		 DBColumn[] result = agg.next();



		/* Filtering on both sides */

//		 ch.epfl.dias.ops.block.Scan scan = new ch.epfl.dias.ops.block.Scan(columnstoreData);
//		 //DBColumn[] x =  scan.execute();
//		 ch.epfl.dias.ops.block.Select sel = new ch.epfl.dias.ops.block.Select(scan, BinaryOp.EQ, 3, 6);
//		 DBColumn[] y =  sel.execute();
//		 int[] columns = {0,2,3,3,6};
//		 ch.epfl.dias.ops.block.Project prj = new ch.epfl.dias.ops.block.Project(sel,columns);
//		//DBColumn[] z =  prj.execute();
//		 ch.epfl.dias.ops.block.ProjectAggregate agg = new ch.epfl.dias.ops.block.ProjectAggregate(sel, Aggregate.SUM, DataType.DOUBLE, 3);
//		 DBColumn[] result = agg.execute();
//		 int output = result[0].getAsInteger()[0];


		// This query should return only one result


		//assertTrue(output == 3);


		 //System.out.println(output);




	}
}
