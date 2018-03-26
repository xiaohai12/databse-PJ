package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import org.omg.CORBA.OBJ_ADAPTER;

import java.util.ArrayList;

public class DBPAXpage {

	// TODO: Implement
    public DBColumn[] minipage;
    public DataType[] types;
    public boolean eof;


    public DBPAXpage(DBColumn[] minipage, DataType[] types)
    {
        this.minipage = minipage;
        this.types = types;
        this.eof = false;
        // this is a constructor used to creat instance with parameter.
    }

    public DBPAXpage() {
        this.eof = true;
    }





}
