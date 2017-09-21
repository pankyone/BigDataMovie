package oracle.demo.oow.bd.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.parser.URLReader;

public class HadoopMovieDAO extends HadoopBaseDAO{
	
    private static Table movieTable = null;
    public final static String TABLE_NAME="MOVIE";
    public final static String CHILD_TABLE = "CastCrew";

    public HadoopMovieDAO() {
        super();
        
    }
    
	public List<MovieTO> getMoviesByTime(String tableId,String time,Table table) {

        List<MovieTO> movieTOList = new ArrayList<MovieTO>();
        String activityValue = null;
        try {
        	table = getTable(HadoopActivityDAO.TABLE_NAME);
			Scan scan = new Scan();
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("time"),
					CompareOp.EQUAL, new SubstringComparator(time));
			((SingleColumnValueFilter) filter).setFilterIfMissing(true);

			scan.setFilter(filter);
			ResultScanner resultScanner = table.getScanner(scan);
			
			for (Result result : resultScanner) 
			{
				if(Bytes.toString(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("tableId"))).equals(tableId))
				{
					JSONObject activityObject = new JSONObject();
					activityObject.put("tableId", tableId);
					activityObject.put("custId",Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("custId"))));
					activityObject.put("movieId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("movieId"))));
					activityObject.put("genreId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("genreId"))));
					activityObject.put("activity", Bytes.toInt(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("activity"))));
					activityObject.put("recommended", Bytes.toString(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("recommended"))));
					activityObject.put("position", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("position"))));
					activityObject.put("price", Bytes.toDouble(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("price"))));
					activityObject.put("rating", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("rating"))));
					activityObject.put("time", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("time"))));
					activityValue = activityObject.toJSONString();
					
					getMovie(activityValue, movieTOList);
				}
			}
			resultScanner.close();
			table.close();
			//Sort them based on the order
	        Collections.sort(movieTOList);
		} 
        catch (Exception e) {
			e.printStackTrace();
		}
        return movieTOList;
	} //getMoviesByTime
	
	public List<MovieTO> getMoviesByKey(int custId,String tableId,Table table) {

        List<MovieTO> movieTOList = new ArrayList<MovieTO>();
        String activityValue = null;
        try {
        	table = getTable(HadoopActivityDAO.TABLE_NAME);
			Scan scan = new Scan();
			Filter filter = new PrefixFilter(Bytes.toBytes(custId+"_"+tableId));

			scan.setFilter(filter);
			ResultScanner resultScanner = table.getScanner(scan);
			
			for (Result result : resultScanner) 
			{

				JSONObject activityObject = new JSONObject();
				activityObject.put("tableId", tableId);
				activityObject.put("custId",custId);
				activityObject.put("movieId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("movieId"))));
				activityObject.put("genreId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("genreId"))));
				activityObject.put("activity", Bytes.toInt(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("activity"))));
				activityObject.put("recommended", Bytes.toString(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("recommended"))));
				activityObject.put("position", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("position"))));
				activityObject.put("price", Bytes.toDouble(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("price"))));
				activityObject.put("rating", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("rating"))));
				activityObject.put("time", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("time"))));
				activityValue = activityObject.toJSONString();
				
				getMovie(activityValue, movieTOList);

			}
			resultScanner.close();
			table.close();
			//Sort them based on the order
	        Collections.sort(movieTOList);
		} 
        catch (Exception e) {
			e.printStackTrace();
		}
        return movieTOList;
	} //getMoviesByKey
	
	private void getMovie(String value, List<MovieTO> movieTOList){
        ActivityTO activityTO = null;
        MovieTO movieTO = null;
        HadoopMovieDAO movieDAO = new HadoopMovieDAO();
        int movieId = 0;
        long timeStamp;

        //The value part of these KV pair is ActivityTO JSONTxt
        activityTO = new ActivityTO(value);

        //System.out.println("JSONTxt: " + activityJsonTxt);

        if (activityTO != null) {
            
            movieId = activityTO.getMovieId();
            timeStamp = activityTO.getTimeStamp();

            //get the movieTO from movieId
            movieTO = movieDAO.getMovieById(movieId);
            if (movieTO != null) {
                //set the timeStamp to movieTO
                movieTO.setOrder(timeStamp);

                //add the movie to the list
                movieTOList.add(movieTO);
            }
        } //if(StringUtil.isNotEmpty(activityJsonTxt)
    }
	
	public MovieTO getMovieById(String movieIdStr) {
        int movieId = 0;
        if (StringUtil.isNotEmpty(movieIdStr)) {
            try {
                movieId = Integer.parseInt(movieIdStr);
            } catch (NumberFormatException ne) {
                movieId = 0;
            } //EOF try/catch
        } //EOF if
        return getMovieById(movieId);
    } //getMovie

    public MovieTO getMovieById(int movieId) {
        List<CastTO> castList = null;
        List<CrewTO> crewList = null;
        HadoopCastDAO castDAO = new HadoopCastDAO();
        HadoopCrewDAO crewDAO = new HadoopCrewDAO();
        CastCrewTO castCrewTO = new CastCrewTO();
        MovieTO movieTO = null;

        try {
        	movieTable = getTable(TABLE_NAME);
			Get get = new Get(Bytes.toBytes(movieId));
			Result result = movieTable.get(get);
			if (!result.isEmpty()) {

			    //Deserialize the movie avro object
				JSONObject movieObj = new JSONObject();
				movieObj.put("id", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("id"))));
				movieObj.put("original_title",Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("original_title"))));
				movieObj.put("overview", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("overview"))));
				movieObj.put("poster_path", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("poster_path"))));
				movieObj.put("release_date", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("release_date"))));
				movieObj.put("vote_count", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("vote_count"))));
				movieObj.put("runtime", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("runtime"))));
				movieObj.put("popularity", Bytes.toDouble(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("popularity"))));
				
				String genres_list = Bytes.toString(result.getValue(Bytes.toBytes("genres"),Bytes.toBytes("genres_list")));
				JSONArray genresArr = JSONArray.parseArray(genres_list);
				movieObj.put("genres", genresArr);
			    movieTO = new MovieTO(movieObj.toJSONString());

			    
			    //If internet connection is not successful then reset the movie-poster
			    if (movieTO != null && !URLReader.isInternetReachable())
			        movieTO.setPosterPath("");

			    //Get Cast Inforamtion and set it to castCrewTO
			    castList = castDAO.getMovieCasts(movieId);
			    castCrewTO.setCastList(castList);
			    //Get Crew Inforamtion and set it to castCrewTO
			    crewList = crewDAO.getMovieCrews(movieId);
			    castCrewTO.setCrewList(crewList);
			    //set castCrewTO to movieTO
			    movieTO.setCastCrewTO(castCrewTO);
			}
			movieTable.close();
		} 
        catch (Exception e) {
			e.printStackTrace();
		}
        return movieTO;
    } //getMovieById
    
    /*public void putTables(List<MovieTO> movieList)
    {
    	try {
			for(MovieTO movieTO : movieList)
			{
				Put put = new Put(Bytes.toBytes(movieTO.getId()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(movieTO.getId()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("original_title"), Bytes.toBytes(movieTO.getTitle()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("overview"), Bytes.toBytes(movieTO.getOverview()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("poster_path"), Bytes.toBytes(movieTO.getPosterPath()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("release_date"), Bytes.toBytes(movieTO.getDate()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("vote_count"), Bytes.toBytes(movieTO.getVoteCount()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("runtime"), Bytes.toBytes(movieTO.getRunTime()));
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("popularity"), Bytes.toBytes(movieTO.getPopularity()));
  
				JSONArray genreArr = new JSONArray();
				for(GenreTO genreTO : movieTO.getGenres())
				{
					JSONObject genreObj = new JSONObject();
					genreObj.put("cid", genreTO.getCid());
					genreObj.put("id", genreTO.getId());
					genreObj.put("name", genreTO.getName());
					genreArr.add(genreObj);
				}
				
				String genres_list = genreArr.toJSONString();
				
				put.addColumn(Bytes.toBytes("genres"), Bytes.toBytes("genres_list"), Bytes.toBytes(genres_list));
				
				movieTable.put(put);
				movieTable.close();
			}
		} 
    	catch (Exception e) {
			e.printStackTrace();
		}
    }*/
}
