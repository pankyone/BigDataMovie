package oracle.demo.oow.bd.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import oracle.demo.oow.bd.util.ConstantsHBase;

public class HadoopBaseDAO {

	private static Connection conn;
    private static Configuration conf;
    
    
    public static Connection getHadoopConnect()
    {
    	if(conn == null)
    	{
    		if(conf == null)
    		{
		    	conf = HBaseConfiguration.create();
				conf.set("hbase.zookeeper.quorum", ConstantsHBase.ZOOKEEPER);
				conf.set("hbase.rootdir", ConstantsHBase.HBASE_ROOT_DIR);
    		}
			try 
			{
				conn = ConnectionFactory.createConnection(conf);
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
    	}
		
		return conn;
    }
    
    public static Table getTable(String tablePath) {

        try 
        {
        	return getHadoopConnect().getTable(TableName.valueOf(tablePath));
        } 
        catch (Exception e) {
        	System.err.println("Failed to get table: " + tablePath);
        }
        return null;
    }
}
