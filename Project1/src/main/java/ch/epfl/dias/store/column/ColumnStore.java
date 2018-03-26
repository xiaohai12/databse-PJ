package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ColumnStore extends Store
{

	// TODO: Add required structures
	DataType[] schema;
	String filename;
	String delimiter;
	ArrayList<DBColumn> DataColum = new ArrayList<DBColumn>();

	public ColumnStore(DataType[] schema, String filename, String delimiter)
	{
		// TODO: Implement
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load()
	{
		// TODO: Implement
		String path = filename;

		try{
			FileReader file = new FileReader(path);
			BufferedReader reader = new BufferedReader(file);
			String line = " ";

			ArrayList<String[]> AllItem = new ArrayList<String[]>();


			while((line = reader.readLine())!=null)
			{
				String[] item = line.split(delimiter);
				AllItem.add(item);
			}

			for(int col=0;col<AllItem.get(0).length;col++)

			{
				Object[] Column = new Object[AllItem.size()];
				for (int row=0;row<AllItem.size();row++)
				{
					Column[row] = transfer(AllItem.get(row)[col],schema[col]);
				}

				DBColumn dbColumn= new DBColumn(Column,schema[col]);
				DataColum.add(dbColumn);
			}

			DBColumn last = new DBColumn();
			DataColum.add(last);

		}catch(Exception e){ }
	}


	@Override
	public DBColumn[] getColumns(int[] columnsToGet)
	{
		// TODO: Implement
		DBColumn[] getcolumns = new DBColumn[columnsToGet.length];
		for(int i=0;i<columnsToGet.length;i++)
		{
			getcolumns[i] = DataColum.get(columnsToGet[i]-1);
		}

		return getcolumns;
	}

}
