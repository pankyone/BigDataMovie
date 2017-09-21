package oracle.demo.oow.bd.dao;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONObject;
import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.FileWriterUtil;


public class HadoopActivityDAO extends HadoopBaseDAO{

	HadoopMovieDAO movieDAO = null;
    
    private static Table activityTable = null;
    public final static String TABLE_NAME="ACTIVITY";


    public HadoopActivityDAO() {
        super();
        movieDAO = new HadoopMovieDAO();
    }
    
    public ActivityTO getActivityTO(int custId, int movieId) {
        ActivityTO activityTO = null;
        
        String tableId = KeyConstant.CUSTOMER_CURRENT_WATCH_LIST;
        //update the current position of the movie into current watch list
        JSONObject activityObj = new JSONObject();

        try {
        	activityTable = getTable(TABLE_NAME);
			Get get = new Get(Bytes.toBytes(custId + "_" + tableId + "_" + movieId));
			Result result = activityTable.get(get);
			if (!result.isEmpty()) 
			{
				activityObj.put("tableId", tableId);
			    activityObj.put("custId", custId);
			    if (movieId > 0)
			    	activityObj.put("movieId",movieId);
			    activityObj.put("genreId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("genreId"))));
				activityObj.put("activity", Bytes.toInt(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("activity"))));
				activityObj.put("recommended", Bytes.toString(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("recommended"))));
				activityObj.put("position", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("position"))));
				activityObj.put("price", Bytes.toDouble(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("price"))));
				activityObj.put("rating", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("rating"))));
				activityObj.put("time", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("time"))));
				String  value = activityObj.toJSONString();
			    activityTO = new ActivityTO(value);
			}
			activityTable.close();
			System.out.println("getActivityTO success!");
		} 
        catch (Exception e) {
			e.printStackTrace();
		}
       
        return activityTO;
    } //getActivityTO
    
    public List<MovieTO> getCustomerCurrentWatchList(int custId) {

        List<MovieTO> movieList = movieDAO.getMoviesByKey(custId,KeyConstant.CUSTOMER_CURRENT_WATCH_LIST,activityTable);
        System.out.println("getCustomerCurrentWatchList success!");
        return movieList;
    }
    
    public List<MovieTO> getCustomerBrowseList(int custId) {

        List<MovieTO> movieList = movieDAO.getMoviesByKey(custId,KeyConstant.CUSTOMER_BROWSE_LIST,activityTable);
        System.out.println("getCustomerBrowseList success!");
        return movieList;
    }

    public List<MovieTO> getCustomerHistoricWatchList(int custId) {

        List<MovieTO> movieList = movieDAO.getMoviesByKey(custId,KeyConstant.CUSTOMER_HISTORICAL_WATCH_LIST,activityTable);
        System.out.println("getCustomerHistoricWatchList success!");
        return movieList;
    }

    public List<MovieTO> getCommonPlayList() {
    	DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String yyyyMMdd = formatter.format(new Date());

        List<MovieTO> movieList = movieDAO.getMoviesByTime(KeyConstant.COMMON_CURRENT_WATCH_LIST,yyyyMMdd,activityTable);
        System.out.println("getCommonPlayList success!");
        return movieList;
    }

    
	public void insertCustomerActivity(ActivityTO activityTO) {
        int custId = 0;
        int movieId = 0;
        ActivityType activityType = null;

        //HadoopCustomerDAO customerDAO = new HadoopCustomerDAO();

        if (activityTO != null) {
            
            custId = activityTO.getCustId();
            movieId = activityTO.getMovieId();

            try {
				if (custId > 0 && movieId > 0) {
				    activityType = activityTO.getActivity();
				    
				    switch (activityType) {
				    case STARTED_MOVIE:
				    	activityTable = getTable(TABLE_NAME);
				        activityTO.setTableId(KeyConstant.CUSTOMER_CURRENT_WATCH_LIST);
				        putMovieActivity(activityTO);
				        activityTable.close();
				        
				        break;
				    case PAUSED_MOVIE:
				    	activityTable = getTable(TABLE_NAME);
				        activityTO.setTableId(KeyConstant.CUSTOMER_CURRENT_WATCH_LIST);
				        putMovieActivity(activityTO);
				        activityTable.close();
				       
				        break;
				    case COMPLETED_MOVIE:
				    	activityTable = getTable(TABLE_NAME);
				        activityTO.setTableId( KeyConstant.CUSTOMER_HISTORICAL_WATCH_LIST);
				        putMovieActivity(activityTO);
				        activityTable.close();
				        break;
				    case RATE_MOVIE:
				    	activityTable = getTable(TABLE_NAME);
				    	activityTO.setTableId(KeyConstant.CUSTOMER_RATING_LIST);
				    	putMovieActivity(activityTO);
				    	activityTable.close();
				        //insert user rating for the movie in the CT_MV table
				        //customerDAO.insertMovieRating(custId, movieId, activityTO);
				        break;
				    case BROWSED_MOVIE:
				        //insert browse information
				    	activityTable = getTable(TABLE_NAME);
				        activityTO.setTableId(KeyConstant.CUSTOMER_BROWSE_LIST);
				        putMovieActivity(activityTO);
						activityTable.close();

				        break;	
					default:
						break;

				    }
				} //if (custId > 0 && movieId > 0)

				if (custId > 0 && movieId == 0) {
				    activityType = activityTO.getActivity();
				    switch (activityType) {
				    case LOGIN:
				    	activityTable = getTable(TABLE_NAME);
				    	loginAndLogout(activityTO);
						activityTable.close();
				        break;
				    case LOGOUT:
				    	activityTable = getTable(TABLE_NAME);
				    	loginAndLogout(activityTO);
						activityTable.close();
				        break;
					default:
						break;
				    }
				}
			} catch (Exception e) {
				// TODO 自动生成的 catch 块
				e.printStackTrace();
			}
        } //if (activityTO != null)

    } //insetCustomerActivity
	
	public void putMovieActivity(ActivityTO activityTO)
	{
		try 
		{
			Put put = new Put(Bytes.toBytes(activityTO.getCustId()+"_"+activityTO.getTableId()+'_'+activityTO.getMovieId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(activityTO.getCustId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(activityTO.getMovieId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes(activityTO.getTableId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(activityTO.getGenreId()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(activityTO.getPosition()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(activityTO.getPrice()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(activityTO.getRating().getValue()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(activityTO.getFormattedTime()));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(activityTO.getActivity().getValue()));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes(activityTO.isRecommended().getValue()));
		
			activityTable.put(put);
			FileWriterUtil.writeOnFile(activityTO.getJsonTxt());
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void loginAndLogout(ActivityTO activityTO)
	{
		try {
			Put put = new Put(Bytes.toBytes(activityTO.getCustId()+"_"+activityTO.getTableId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(activityTO.getCustId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(activityTO.getMovieId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes(activityTO.getTableId()));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(activityTO.getGenreId()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(activityTO.getPosition()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(activityTO.getPrice()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(activityTO.getRating().getValue()));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(activityTO.getFormattedTime()));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(activityTO.getActivity().getValue()));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes(activityTO.isRecommended().getValue()));

			Get get = new Get(Bytes.toBytes(activityTO.getCustId()+"_"+activityTO.getTableId()));
			Result result = activityTable.get(get);
			String time = null;
			if(!result.isEmpty())
			{
				time = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("time")));
				activityTable.checkAndPut(Bytes.toBytes(activityTO.getCustId()+"_"+activityTO.getTableId()), Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(time), put);
			}
			else
			{
				activityTable.put(put);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void putMyData()
	{
		try 
		{
			activityTable = getTable(TABLE_NAME);
			List<Put> putlist = new ArrayList<Put>();
			Put put = new Put(Bytes.toBytes("1255601_CT_CWL_857"));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(1255601));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(857));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes("CT_CWL"));
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(7));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(0));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(0.0));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(0));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes("2017-07-06:14:51:48"));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(3));
			put.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes("Y"));
		
			putlist.add(put);
			
			Put put1 = new Put(Bytes.toBytes("1255601_CT_CWL_3989"));
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(1255601));
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(3989));
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes("CT_CWL"));
			put1.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(7));
			put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(0));
			put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(0.0));
			put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(0));
			put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes("2017-07-06:14:54:16"));
			put1.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(4));
			put1.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes("Y"));
		
			putlist.add(put1);
			
			Put put2 = new Put(Bytes.toBytes("1255601_CT_HWL_707"));
			put2.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(1255601));
			put2.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(707));
			put2.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes("CT_HWL"));
			put2.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(9));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(0));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(0.0));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(0));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes("2017-07-06:14:58:34"));
			put2.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(2));
			put2.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes("Y"));
		
			putlist.add(put2);
			
			Put put3 = new Put(Bytes.toBytes("1255601_CM_CWL_5551"));
			put3.addColumn(Bytes.toBytes("id"), Bytes.toBytes("custId"), Bytes.toBytes(1255601));
			put3.addColumn(Bytes.toBytes("id"), Bytes.toBytes("movieId"), Bytes.toBytes(5551));
			put3.addColumn(Bytes.toBytes("id"), Bytes.toBytes("tableId"), Bytes.toBytes("CM_CWL"));
			put3.addColumn(Bytes.toBytes("id"), Bytes.toBytes("genreId"), Bytes.toBytes(11));
			put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("position"), Bytes.toBytes(0));
			put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(0.0));
			put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rating"), Bytes.toBytes(0));
			put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes("2017-07-06:15:15:24"));
			put3.addColumn(Bytes.toBytes("type"), Bytes.toBytes("activity"), Bytes.toBytes(6));
			put3.addColumn(Bytes.toBytes("type"), Bytes.toBytes("recommended"), Bytes.toBytes("Y"));
		
			putlist.add(put3);
			
			activityTable.put(putlist);
			activityTable.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
