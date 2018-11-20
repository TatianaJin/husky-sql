package husky.sql.calcite.table;


import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;

//import jdk.nashorn.internal.parser.JSONParser;


public class SimplePartsTable extends AbstractTable implements ScannableTable{
	private String name;
	private String JSONTablePath;
	private ArrayList<Column> columns;
	private Object[][] rows;
	private String Url;


	public static class Column{
		private int id;
		private String name;
		private String datatype;

		public Column(int id, String name, String datatype){
			this.id = id;
			this.name = name;
			this.datatype = datatype;
		}

		public String getDatatype(){
		    return this.datatype;
        }

        public String getName(){
		    return this.name;
        }
	}




	public String toString() { return "SimplePartsTable"; }

	public Enumerable<Object[]> scan(final DataContext root) {
		return Linq4j.asEnumerable(rows);
	}



	public SimplePartsTable(String JSONTablePath) {

        this.JSONTablePath = JSONTablePath;
        this.columns = new ArrayList<Column>();
        try {

            JSONParser parser = new JSONParser();
            JSONObject table = (JSONObject) parser.parse(new FileReader(JSONTablePath));
            this.name = (String) table.get("name");
            this.Url = (String) table.get("url");

            JSONArray columns = (JSONArray) table.get("columns");

            for (Object col : columns) {
                JSONObject column = (JSONObject) col;
                String id = (String) column.get("id");
                String datatype = (String) column.get("datatype");
                String name = (String) column.get("name");
                Column column_instance = new Column(Integer.valueOf(id), name, datatype);
                this.columns.add(column_instance);
            }


        } catch (FileNotFoundException fe) {
            fe.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private SqlTypeName getColumnType(String datatype) {
	    SqlTypeName type;
        switch(datatype) {
            case "int":
                type = SqlTypeName.INTEGER;
                break;
            case "double":
                type = SqlTypeName.DOUBLE;
                break;
            case "float":
                type = SqlTypeName.FLOAT;
                break;
            case "varchar":
                type = SqlTypeName.VARCHAR;
                break;
            case "bigint":
                type = SqlTypeName.BIGINT;
                break;
            default:
                type = SqlTypeName.ANY;
                break;
        }
        return type;
    }


    private final RelProtoDataType protoDataType = new RelProtoDataType() {
            @Override
            public RelDataType apply(RelDataTypeFactory a0) {
                Builder builder = a0.builder();
                for(Column col: columns){
                    SqlTypeName type = getColumnType(col.getDatatype());
                    if(col.getDatatype().equals("varchar")){
                        builder.add(col.getName(), type, 10);
                        continue;
                    }
                    builder.add(col.getName(), type);
                }
                return builder.build();
            }
	};

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return protoDataType.apply(typeFactory);
    }


    public String getUrl(){return this.Url;}


}