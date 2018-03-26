package ch.epfl.dias.store.PAX;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import sun.security.pkcs11.Secmod;

public class PAXStore extends Store {

	// TODO: Add required structures
	DataType[] schema;
	String filename;
	String delimiter;
	int tuplesPerPage;
	ArrayList<DBPAXpage> DataPAX=new ArrayList<DBPAXpage>();
	int Totalrow;

	private int PageNum;


	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		// TODO: Implement

		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() {
		// TODO: Implement

        ColumnStore ColumnData = new ColumnStore(schema,filename,delimiter);
        int[] co ={1};
        ColumnData.load();
        Totalrow = ColumnData.getColumns(co)[0].attributes.length;
		PageNum = (int)Math.ceil((double)Totalrow/tuplesPerPage)+1;
        int[] columns = new int[schema.length];
        for(int i=0;i<schema.length;i++)
        {
            columns[i] = i+1;
        }

        DBColumn[] Columns = ColumnData.getColumns(columns);

        for(int page=1;page<PageNum;page++)
		{
			DBColumn[] TemColumns = new DBColumn[columns.length];
			for(int column=0;column<columns.length;column++)
			{
				int endIndex = page *tuplesPerPage;
				if(endIndex>Columns[column].attributes.length)
				{
					endIndex = Columns[column].attributes.length;
				}
				int startIndex = (page-1)*tuplesPerPage;
				Object[] Attribute = Arrays.copyOfRange(Columns[column].attributes,startIndex,endIndex);
				DataType type = Columns[column].type;
				TemColumns[column] = new DBColumn(Attribute,type);
			}

			DBPAXpage Page = new DBPAXpage(TemColumns,schema);
			DataPAX.add(Page);
		}

		DBPAXpage Pagelast = new DBPAXpage();
		DataPAX.add(Pagelast);
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		if (rownumber>Totalrow)
		{
			return null;
		}
		else
		{
			rownumber = rownumber;
			int page =(int) Math.ceil((double)rownumber/tuplesPerPage);
			int row = rownumber-(page-1)*tuplesPerPage;
			DBColumn[] Minipage = DataPAX.get(page-1).minipage;
			int size = Minipage.length;
			Object[] rowdata = new Object[size];
			for(int i=0;i<size;i++)
			{

				rowdata[i] = Minipage[i].attributes[row-1];
			}
			DBTuple return_row = new DBTuple(rowdata,schema);


			return return_row;
		}

	}
}
