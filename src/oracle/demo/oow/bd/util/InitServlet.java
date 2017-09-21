package oracle.demo.oow.bd.util;

import java.io.FileInputStream;
import java.util.Properties;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

/**
 * Servlet implementation class InitServlet
 */
public class InitServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
	@Override
    public void init(ServletConfig config) throws ServletException{
        super.init(config);
        String path;
        FileInputStream fis;
        try{
        	path = InitServlet.class.getResource("/").getPath();
        	fis = new FileInputStream(path+"conf.properties");
        	Properties properties = new Properties();
        	properties.load(fis);
        	fis.close();
        	
        	String outputFile = properties.getProperty("output_file");
        	if(outputFile!=null)
        	{
        		FileWriterUtil.OUTPUT_FILE = outputFile;
        	}
        	
        	String zookeeper = properties.getProperty("hbase.zookeeper.quorum");
        	if(zookeeper!=null)
        	{
        		ConstantsHBase.ZOOKEEPER = zookeeper;
        	}
        	
        	String hbaseRootDir = properties.getProperty("hbase.rootdir");
        	if(hbaseRootDir!=null)
        	{
        		ConstantsHBase.HBASE_ROOT_DIR = hbaseRootDir;
        	}
        	
        	String db_username = properties.getProperty("mysql.username");
        	if(db_username!=null)
        	{
        		ConstantsHBase.MYSQL_USERNAME = db_username;
        	}
        	
        	String db_password = properties.getProperty("mysql.password");
        	if(hbaseRootDir!=null)
        	{
        		ConstantsHBase.MYSQL_PASSWORD = db_password;
        	}
        	
        	String url = properties.getProperty("mysql.url");
        	if(url!=null)
        	{
        		ConstantsHBase.MYSQL_URL = url;
        	}
        	
        	String driver = properties.getProperty("mysql.driver");
        	if(driver!=null)
        	{
        		ConstantsHBase.MYSQL_DRIVER = driver;
        	}
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
    }
}
