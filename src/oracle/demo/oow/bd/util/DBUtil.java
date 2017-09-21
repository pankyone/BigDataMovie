package oracle.demo.oow.bd.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {

	static String db_username = ConstantsHBase.MYSQL_USERNAME;
	static String db_password = ConstantsHBase.MYSQL_PASSWORD;
	static String db_url = ConstantsHBase.MYSQL_URL;
	static String db_driver = ConstantsHBase.MYSQL_DRIVER;
	
	public static Connection getConn() 
	{
		Connection conn = null;
		try 
		{
			Class.forName(db_driver);
			conn = DriverManager.getConnection(db_url, db_username, db_password);
			System.out.println("Connection链接成功");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void close(Statement state, Connection conn) 
	{
		if(state!=null) 
		{
			try 
			{
				state.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		if(conn!=null) 
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void close(ResultSet rs, Statement state, Connection conn) 
	{
		if(rs!=null) 
		{
			try 
			{
				rs.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		if(state!=null) 
		{
			try 
			{
				state.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		if(conn!=null) 
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
}
