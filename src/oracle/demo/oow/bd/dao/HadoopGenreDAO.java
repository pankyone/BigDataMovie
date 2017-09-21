package oracle.demo.oow.bd.dao;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONObject;
import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.to.GenreTO;


public class HadoopGenreDAO extends HadoopBaseDAO{

    private static Table genreTable = null;
    public final static String TABLE_NAME="GENRE";
    
    public HadoopGenreDAO() {
        super();
        
    }
    
    public List<GenreTO> getGenres() {
    	List<GenreTO> genreList = new ArrayList<GenreTO>();
    	try {
	        String genreTOValue = null;        
	        GenreTO genreTO = null;
	
	        
	        String cid = KeyConstant.GENRE_TABLE;
	        genreTable = getTable(TABLE_NAME);
	        Scan scan = new Scan();
			
		    Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("cid"),
		    		CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes(cid)));
		    ((SingleColumnValueFilter) filter).setFilterIfMissing(true);
		    scan.setFilter(filter);
		    
			ResultScanner resultScanner = genreTable.getScanner(scan);
			
			for (Result result : resultScanner) {
				JSONObject genreObj = new JSONObject();
				for(Cell cell:result.rawCells())
				{
					if(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()).equals("id"))
					{
						genreObj.put("id",Bytes.toInt(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
					}
					else
					{
						genreObj.put(new String(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()), Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
					}
				}
				genreTOValue = genreObj.toJSONString();
				genreTO = new GenreTO(genreTOValue);
	            //Add TO to the list
	            genreList.add(genreTO);
			}
			resultScanner.close();
			genreTable.close();
			
			System.out.println("getGenres success!");
		} catch (Exception e) {
			e.printStackTrace();
		}
        return genreList;
    }
    
  /*  public void putGenres(GenreTO genreTO)
    {
    	try {
    		String id = String.valueOf("GN"+"_"+genreTO.getId());
    		Put put = new Put(Bytes.toBytes(id));
    		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cid"), Bytes.toBytes("GN"));
    		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(genreTO.getId()));
    		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(genreTO.getName()));
    		genreTable.put(put);
    		//genreTable.close();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }*/

}
