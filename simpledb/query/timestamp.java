package simpledb.query;
// importing SimpleDateFormat to parse timestamps
import java.text.SimpleDateFormat;
// In case input string is not a timestamp 
import java.text.ParseException;
import java.util.Date;

/**
 * The class that wraps Java Date as database constants.
 * @author Edward Sciore
 */
public class timestamp implements Constant {
   private Date val;
   // object to format and parse 'timestamps'
   private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   
   /**
    * Create a constant by wrapping the specified string.
    * @param s the string value
    */
   public timestamp(String s) throws ParseException {
      val = simpleDateFormat.parse(s);
   }

   /**
    * Create a constant using a date created from milliseconds since 1970-01-01 00:00:00
    * @param l the long value
    */
   public timestamp(Long l) {
      val = new Date(l);
   }
   
   /**
    * Unwraps the string and returns it.
    * @see simpledb.query.Constant#asJavaVal()
    */
   public Date asJavaVal() {
      return val;
   }
   
   public boolean equals(Object obj) {
      timestamp t = (timestamp) obj;
      return t != null && val.equals(t.val);
   }

   public int compareTo(Constant obj) {
      timestamp t = (timestamp) obj;
     // System.out.println(val+" Comparing with "+t.asJavaVal());
      //System.out.print(val);
     // System.out.println(val.compareTo(t.val));
      return val.compareTo(t.val);
   }

   public int hashCode() {
      return val.hashCode();
   }
   
   public String toString() {
      return simpleDateFormat.format(val);
   }
}
