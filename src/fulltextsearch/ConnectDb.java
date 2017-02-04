package fulltextsearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectDb {
   private static String driveClassName="com.mysql.jdbc.Driver";
   private static String url="jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8";
   private static String user="root";
   private static String password="root";
   
   public static Connection Connect(){
	   Connection conn=null;
	   try{
		   Class.forName(driveClassName);
		   System.out.println("connect successfully!");
	   }catch(ClassNotFoundException e){
		   System.out.println("load driver failed!");
		   e.printStackTrace();
	   }
	   try{
		   conn=DriverManager.getConnection(url,user,password);
	   }catch(SQLException e){
		   System.out.println("connect failed!");
		   e.printStackTrace();
	   }
	   return conn;
   }
}
