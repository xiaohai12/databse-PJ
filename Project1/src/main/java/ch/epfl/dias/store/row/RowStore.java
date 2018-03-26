package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.ArrayList;

public class RowStore extends Store
{

	// TODO: Add required structures
    DataType[] schema;
    String filename = new String();
    String delimiter = new String();
	ArrayList<DBTuple> DataRow=new ArrayList<DBTuple>();

	public RowStore(DataType[] schema, String filename, String delimiter)
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


		String Path = filename;

		try
		{
			FileReader file = new FileReader(Path);
			BufferedReader reader = new BufferedReader(file);
			String line = " ";

			while((line = reader.readLine())!=null)
			{
				String[] item = line.split(delimiter);
				Object[] Item = new Object[item.length];

				for(int i=0; i<item.length;i++)
				{
					Item[i] = transfer(item[i],schema[i]);
				}

				DBTuple dbTuple = new DBTuple(Item,schema);
				DataRow.add(dbTuple);
			}

			DBTuple last = new DBTuple();
			DataRow.add(last);

		}catch(Exception e) { }
	}

	@Override
	public DBTuple getRow(int row_number)
	{
		// TODO: Implement
		try{
			return DataRow.get(row_number-1);
		}catch (Exception e){return null;}


	}


}
