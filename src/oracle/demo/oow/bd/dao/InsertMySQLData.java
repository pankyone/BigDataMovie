package oracle.demo.oow.bd.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Statement;

import oracle.demo.oow.bd.util.DBUtil;

public class InsertMySQLData {
	
	public static void putData() throws Exception
    {
		Connection conn = DBUtil.getConn();
		Statement state = conn.createStatement();
    	File file = new File("F:\\cust_rating.txt");
    	BufferedReader br = new BufferedReader(new FileReader(file));
    	String s = null;
    	while((s = br.readLine())!=null)
    	{
    		String[] nodes = s.split(",");
    		String sql = "insert into CUSTRATING values("+nodes[0]+","+nodes[1]+","+nodes[2]+")";
    		int i = state.executeUpdate(sql);
    		if(i==1)
    		{
    			System.out.println("Insert Success!");
    		}
    	}
    	br.close();
    	DBUtil.close(state, conn);
    }

}
