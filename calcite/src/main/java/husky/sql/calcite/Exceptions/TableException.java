package husky.sql.calcite.Exceptions;

public class TableException extends RuntimeException {


     public TableException(String msg, Throwable cause) {
         super(msg, cause);
     }

     public TableException(String msg) {
         super(msg, null);
     }
}
