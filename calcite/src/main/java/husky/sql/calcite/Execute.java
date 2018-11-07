package husky.sql.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.model.ModelHandler;

import com.google.common.io.Resources;

import husky.sql.calcite.planner.SimpleQueryPlanner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class Execute {
  public static void main(String[] args) throws IOException, SQLException {
    if (args.length < 1) {
      System.out.println("usage: ./SimpleQueryPlanner \"<query string>\"");
    }
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    CalciteConnection connection = DriverManager.getConnection("jdbc:calcite:", info)
                                                .unwrap(CalciteConnection.class);
    String schema = Resources.toString(SimpleQueryPlanner.class.getResource("/model.json"),
                                       Charset.defaultCharset());
    // ModelHandler reads the schema and load the schema to connection's root schema and sets the default schema
    new ModelHandler(connection, "inline:" + schema);

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(args[0]);
    ResultSetMetaData rsmd = resultSet.getMetaData();
    int columnsNumber = rsmd.getColumnCount();
    // Print column names
    System.out.print(rsmd.getColumnName(1));
    for (int i = 2; i <= columnsNumber; i++) {
      System.out.print(" | " + rsmd.getColumnName(i));
    }
    System.out.println();

    while (resultSet.next()) {
      System.out.format("%" + rsmd.getColumnName(1).length() + "s", resultSet.getString(1));
      for (int i = 2; i <= columnsNumber; i++) {
        System.out.print(" | ");
        System.out.format("%" + rsmd.getColumnName(i).length() + "s", resultSet.getString(i));
      }
      System.out.println();
    }

    resultSet.close();
    statement.close();
    connection.close();
  }
}
