package oracle.demo.oow.bd.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;


public class HadoopCastDAO extends HadoopBaseDAO{
	
	private static Table castTable = null;
    private static Table crewTable = null;
    public final static String TABLE_NAME="CAST";
    
    public HadoopCastDAO() {
        super();  
    }
    
    public List<CastTO> getMovieCasts(int movieId) {

        List<CastTO> castList = null;
        CastCrewTO castCrewTO = null;
        String jsonTxt = null;

        try {
			if (movieId > -1) {
				
				jsonTxt = getMovieCastCrew(movieId,castTable,crewTable);
			    if (StringUtil.isNotEmpty(jsonTxt)) {
			        castCrewTO = new CastCrewTO(jsonTxt.trim());
			        //Create CastCrewTO
			        castList = castCrewTO.getCastList();
			        /**
			         * Sort the Movie Cast based on the order
			         */
			        Collections.sort(castList);
			    } //if(StringUtil.isNotEmpty(jsonTxt))
			} //if (movieId > -1)
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
        return castList;
    } //getMovieCasts
    
    public String getMovieCastCrew(int movieId,Table castTable,Table crewTable)
    {
    	String jsonTxt = null;
    	JSONObject castCrewObj = null;
    	try 
    	{
			if (movieId > 0) 
			{
				castTable = getTable(TABLE_NAME);
			    crewTable = getTable(HadoopCrewDAO.TABLE_NAME);
				Scan scan = new Scan();
				String value = "{\"id\":"+movieId+",\"order\":1,\"character\":\"\"}";
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes("movies"), Bytes.toBytes("movies_list"),
						CompareOp.EQUAL, new SubstringComparator(value));
				((SingleColumnValueFilter) filter).setFilterIfMissing(true);
				scan.setFilter(filter);
				ResultScanner castResultScanner = castTable.getScanner(scan);
				
				JSONArray castArr = new JSONArray();;
				JSONObject castObj = null;
				JSONArray castMoviesArr = null;
				
				for (Result result : castResultScanner) 
				{
					
					int id = Bytes.toInt(result.getValue(Bytes.toBytes("cast"),Bytes.toBytes("id")));
					String name = Bytes.toString(result.getValue(Bytes.toBytes("cast"),Bytes.toBytes("name")));
					String movies = Bytes.toString(result.getValue(Bytes.toBytes("movies"),Bytes.toBytes("movies_list")));
					castMoviesArr = JSONArray.parseArray(movies);
					for(int i=0;i<castMoviesArr.size();i++)
					{
						if(movieId==castMoviesArr.getJSONObject(i).getIntValue("id"))
						{
							castObj = new JSONObject();
							castObj.put("id", id);
							castObj.put("name", name);
							castObj.put("movies", castMoviesArr);
							castArr.add(castObj);
							break;
						}
					}
				}
				JSONArray crewArr = new JSONArray();;
				JSONObject crewObj = null;
				JSONArray crewMoviesArr = null;

				filter = new SingleColumnValueFilter(Bytes.toBytes("movies"), Bytes.toBytes("movies_list"),
						CompareOp.EQUAL, new SubstringComparator(String.valueOf(movieId)));
				((SingleColumnValueFilter) filter).setFilterIfMissing(true);
				scan.setFilter(filter);
				ResultScanner crewResultScanner = crewTable.getScanner(scan);
				
				for (Result result : crewResultScanner) 
				{
					int id = Bytes.toInt(result.getValue(Bytes.toBytes("crew"),Bytes.toBytes("id")));
					String name = Bytes.toString(result.getValue(Bytes.toBytes("crew"),Bytes.toBytes("name")));
					String job = Bytes.toString(result.getValue(Bytes.toBytes("crew"),Bytes.toBytes("job")));
					String movies = Bytes.toString(result.getValue(Bytes.toBytes("movies"),Bytes.toBytes("movies_list")));
					crewMoviesArr = JSONArray.parseArray(movies);
					for(int i=0;i<crewMoviesArr.size();i++)
					{
						if(movieId==crewMoviesArr.getJSONObject(i).getIntValue("id"))
						{
							crewObj = new JSONObject();
							crewObj.put("id", id);
							crewObj.put("name", name);
							crewObj.put("job", job);
							crewObj.put("movies", crewMoviesArr);
							crewArr.add(crewObj);
							break;
						}
					}
				}
				if(!castObj.isEmpty()||!crewObj.isEmpty())
				{
					castCrewObj = new JSONObject();
					castCrewObj.put("id", movieId);
					castCrewObj.put("mid", movieId);
					castCrewObj.put("cast", castArr);
					castCrewObj.put("crew", crewArr);
					jsonTxt = castCrewObj.toJSONString();
				}
				castResultScanner.close();
				crewResultScanner.close();
				castTable.close();
			    crewTable.close();
			}
		} 
    	catch (Exception e) {
			e.printStackTrace();
		}
    	return jsonTxt;
    }
    
    public List<MovieTO> getMoviesByCast(int castId) {
        List<CastMovieTO> castMovieList = null;
        List<MovieTO> movieList = new ArrayList<MovieTO>();
        int movieId = 0;
        CastTO castTO = null;
        MovieTO movieTO = null;
        String castTOValue = null;
        HadoopMovieDAO movieDAO = new HadoopMovieDAO();
        JSONArray castArr = null;
        JSONObject castObj = null;

        if(castId > 0)
        {
        	castTable = getTable(TABLE_NAME);
        	Scan scan = new Scan();
        	Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(castId)));
        	scan.setFilter(filter);
        	ResultScanner resultScanner = null;
			try 
			{
				resultScanner = castTable.getScanner(scan);
			
	        	for (Result result : resultScanner) 
	        	{
	        		int id = Bytes.toInt(result.getValue(Bytes.toBytes("cast"),Bytes.toBytes("id")));
					String name = Bytes.toString(result.getValue(Bytes.toBytes("cast"),Bytes.toBytes("name")));
					String movies = Bytes.toString(result.getValue(Bytes.toBytes("movies"),Bytes.toBytes("movies_list")));
					castArr = JSONArray.parseArray(movies);
					castObj = new JSONObject();
					castObj.put("id", id);
					castObj.put("name", name);
					castObj.put("movies", castArr);
	        	}
	        	
	        	resultScanner.close();
	        	castTable.close();
			} 
        	catch (Exception e)
			{
				e.printStackTrace();
			}
        	if(!castObj.isEmpty()&&castObj!=null)
        	{
        		castTOValue = castObj.toJSONString();
        	
	            castTO = new CastTO(castTOValue);
	            if (castTO != null) {
	                castMovieList = castTO.getCastMovieList();
	
	                for (CastMovieTO castMovieTO : castMovieList) {
	                    movieId = castMovieTO.getId();
	                    movieTO = movieDAO.getMovieById(movieId);
	
	                    if (movieTO != null) {
	                        //add to movieList
	                        movieList.add(movieTO);
	                    } //if(movieTO!=null)
	                } //for
	            } //if(cast!=null)
        	}
        }
        return movieList;
    }
    
    /*public static void putCASTTable() throws Exception
    {
    	Table castTable = getTable(TABLE_NAME);
    	File file = new File("F:\\movie-cast.txt");//Text文件
    	BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
    	String s = null;
    	while((s = br.readLine())!=null){//使用readLine方法，一次读一行
    		
			JSONObject obj = JSONObject.parseObject(s);
			JSONArray arr = obj.getJSONArray("movies");
			String str = JSONArray.toJSONString(arr);
    		Put put = new Put(Bytes.toBytes(obj.getIntValue("id")));
    		put.addColumn(Bytes.toBytes("cast"), Bytes.toBytes("id"), Bytes.toBytes(obj.getIntValue("id")));
    		put.addColumn(Bytes.toBytes("cast"), Bytes.toBytes("name"), Bytes.toBytes(obj.getString("name")));
    		put.addColumn(Bytes.toBytes("movies"), Bytes.toBytes("movies_list"), Bytes.toBytes(str));
    		castTable.put(put);
    	}
    	br.close();
    	castTable.close();
    }*/
}
