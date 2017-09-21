package oracle.demo.oow.bd.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerGenreMovieTO;
import oracle.demo.oow.bd.to.CustomerGenreTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.to.ScoredGenreTO;
import oracle.demo.oow.bd.util.StringUtil;

public class HadoopCustomerDAO extends HadoopBaseDAO{

	private static Table customerTable = null;
    public final static String TABLE_NAME="CUSTOMER";
    public final static String CHILD_TABLE="customerGenres";
    public final static String CUSTOMER_GENRE_MOVIE_TABLE = "customerGenreMovie";
    
    private static int MOVIE_MAX_COUNT = 25;
    private static int GENRE_MAX_COUNT = 10;
    
    public HadoopCustomerDAO() {
        super();
    	
    }
    
	public CustomerTO getCustomerByCredential(String username, String password) {
		CustomerTO customerTO = null;
    	try {
    		customerTable = getTable(TABLE_NAME);
    		Scan scan = new Scan();
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(username)));
			scan.setFilter(filter);
			
			ResultScanner resultScanner = customerTable.getScanner(scan);

			for(Result result : resultScanner)
			{
				String customer_username = Bytes.toString(result.getValue(Bytes.toBytes("user"),Bytes.toBytes("username")));
				String customer_pwd = Bytes.toString(result.getValue(Bytes.toBytes("user"),Bytes.toBytes("password")));

				if(username.equals(customer_username)&&password.equals(customer_pwd))
				{
					JSONObject customerObject = new JSONObject();
					customerObject.put("id", Integer.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("user"),Bytes.toBytes("id")))));
					customerObject.put("name",Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"))));
					customerObject.put("email", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("email"))));
					customerObject.put("username", customer_username);
					customerObject.put("password", customer_pwd);
					customerTO = new CustomerTO(customerObject.toJSONString());
				}
			}
			customerTable.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return customerTO;
    }
	
	public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId) {
        return getMovies4CustomerByGenre(custId, genreId, MOVIE_MAX_COUNT);
    }

    public List<MovieTO> getMovies4CustomerByGenre(int custId, int genreId, int maxCount) {

        List<MovieTO> movieList = null;
		try {
			movieList = new ArrayList<MovieTO>();
			MovieTO movieTO = null;
			ActivityTO activityTO = null;
			HadoopMovieDAO movieDAO = new HadoopMovieDAO();
			String value = null;
			int movieId = 0;
			int count = 0;
			ResultScanner resultScanner = null;
			Table customerGenreMovieTable = null;
			if (genreId > 0)
			{
				customerGenreMovieTable = getTable(TABLE_NAME + "." + CUSTOMER_GENRE_MOVIE_TABLE);
				Scan scan = new Scan();
				Filter filter = new PrefixFilter(Bytes.toBytes(custId + "_" + genreId));
				scan.setFilter(filter);
				resultScanner = customerGenreMovieTable.getScanner(scan);
				
				if(resultScanner.next().isEmpty())
				{
					filter = new PrefixFilter(Bytes.toBytes(0 + "_" + genreId));
					scan.setFilter(filter);
					resultScanner = customerGenreMovieTable.getScanner(scan);
				}
			}
			//Now the best attempt is made to get the recommended movies for the
			//customer is done lets fetch the MovieTO
			for(Result result : resultScanner) {

			    JSONObject customerGenreMovieObj = new JSONObject();
			    customerGenreMovieObj.put("id", custId);
			    customerGenreMovieObj.put("genreId", genreId);
			    customerGenreMovieObj.put("movieId", Bytes.toInt(result.getValue(Bytes.toBytes("customerGenre"), Bytes.toBytes("movieId"))));
			    value = customerGenreMovieObj.toJSONString();
			    
			    CustomerGenreMovieTO custGM = new CustomerGenreMovieTO(value);
			    
			    movieId = custGM.getMovieId();
			    //get MovieTO by movieId

			    movieTO = movieDAO.getMovieById(movieId);
			    if (movieTO!=null) {

			        /**
			         * Check to see if movie poster is available. If it is then give a
			         * score of 100 otherwise 0. This would help ordering movies with
			         * posters on the top of the list.
			         */
			    	
			        if (StringUtil.isNotEmpty(movieTO.getPosterPath())) {
			            movieTO.setOrder(100);
			        } else {
			            movieTO.setOrder(0);
			        }

			        //Check to see if user has already rated this movie
			        activityTO = this.getMovieRating(custId, movieId);
			        if (activityTO != null) {
			            movieTO.setUserRating(activityTO.getRating());
			        }
			        //add movieTO to the list
			        movieList.add(movieTO);
			        //check if count is less than or equals to maxCount or not
			        count++;
			        if (count >= maxCount) {
			            break;
			        }
			    } //if(movieTO!=null)
			    resultScanner.close();
			    customerGenreMovieTable.close();
			} //EOF while

			//Sort the movie list
			Collections.sort(movieList);
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
        return movieList;
    } //getMovie4CustomerByGenre
    
	public List<GenreMovieTO> getMovies4Customer(int custId){
        return getMovies4Customer(custId, MOVIE_MAX_COUNT, GENRE_MAX_COUNT);
    }

    public List<GenreMovieTO> getMovies4Customer(int custId, int movieMaxCount, int genreMaxCount){

        List<GenreMovieTO> genreMovieList = null;
		try {
			genreMovieList = new ArrayList<GenreMovieTO>();
			int genreId = 0;
			String name = null;
			GenreMovieTO genreMovieTO = null;
			GenreTO genreTO = null;
			List<MovieTO> movieList = null;
			int count = 0;
			String jsonTxt=null;
			
			Table customerGenresTable = getTable(TABLE_NAME+"."+CHILD_TABLE);
			Get get = new Get(Bytes.toBytes(custId));
			Result result = customerGenresTable.get(get);
			JSONObject customerGenresObj = null;
			if(!result.isEmpty())
			{
				customerGenresObj = new JSONObject();
				customerGenresObj.put("id", custId);
				customerGenresObj.put("cid", custId);
				String genres_list = Bytes.toString(result.getValue(Bytes.toBytes("genres"),Bytes.toBytes("genres_list")));
				JSONArray genresArr = JSONArray.parseArray(genres_list);
				customerGenresObj.put("genres", genresArr);
			}    
			else
			{
				customerGenresObj = new JSONObject();
				get = new Get(Bytes.toBytes(0));
				result = customerGenresTable.get(get);
				customerGenresObj.put("id", 0);
				customerGenresObj.put("cid", 0);
				String genres_list = Bytes.toString(result.getValue(Bytes.toBytes("genres"),Bytes.toBytes("genres_list")));
				JSONArray genresArr = JSONArray.parseArray(genres_list);
				customerGenresObj.put("genres", genresArr);
			}
			
			jsonTxt = (customerGenresObj!=null?customerGenresObj.toJSONString():null);

			if (StringUtil.isNotEmpty(jsonTxt)) {

			    CustomerGenreTO customerGenreTO = new CustomerGenreTO(jsonTxt);
			    for (ScoredGenreTO scoredGenreTO : customerGenreTO.getScoredGenreList()) {
			        //create genreMovieTO object
			        genreMovieTO = new GenreMovieTO();
			        //Get values from ScoredGenreTO and assign it to GenreTO
			        genreId = scoredGenreTO.getId();
			        name = scoredGenreTO.getName();
			        //create GenreTO
			        genreTO = new GenreTO();
			        genreTO.setId(genreId);
			        genreTO.setName(name);
			        //get Movie list by genre
			        movieList = this.getMovies4CustomerByGenre(custId, genreId, movieMaxCount);
			        //set GenreTO & MovieTO list to GenreMovieTO
			        genreMovieTO.setGenreTO(genreTO);
			        genreMovieTO.setMovieList(movieList);
			        //add genreMovieTO to the list
			        genreMovieList.add(genreMovieTO);
			        //Break the loop if you have got M top genres from the list
			        count++;
			        if (count >= genreMaxCount) {
			            break;
			        }

			    } //for (ScoredGenreTO scoredGenreTO
			} else {
			    System.out.println("Error: Default recommendation data is not fed into DB yet:\nPlease run MovieDAO.insertTopMoviesPerGenre() method first to seed the default recommendation.");
			} //if(StringUtil.isNotEmpty(jsonTxt))

			customerGenresTable.close();
			System.out.println("getMovies4Customer");
		} catch (Exception e) {
			e.printStackTrace();
		}
        return genreMovieList;
    } //getCustomerMoviesByGenre
	
    public ActivityTO getMovieRating(int custId, int movieId) {
    	ActivityTO activityTO = null;
		try {
			String jsonTxt = null;
			activityTO = null;
			JSONObject activityObj = null;
			Table activityTable = getTable(HadoopActivityDAO.TABLE_NAME);
			if (custId > 0 && movieId > 0) 
			{
				Get get = new Get(Bytes.toBytes(custId+"_"+"CLICK"+'_'+movieId));
				Result result = null;

				result = activityTable.get(get);

				if(!result.isEmpty())
				{
					activityObj = new JSONObject();
					activityObj.put("tableId", "CLICK");
				    activityObj.put("custId", custId);
				    activityObj.put("movieId",movieId);
				    activityObj.put("genreId", Bytes.toInt(result.getValue(Bytes.toBytes("id"),Bytes.toBytes("genreId"))));
					activityObj.put("activity", Bytes.toInt(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("activity"))));
					activityObj.put("recommended", Bytes.toString(result.getValue(Bytes.toBytes("type"),Bytes.toBytes("recommended"))));
					activityObj.put("position", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("position"))));
					activityObj.put("price", Bytes.toDouble(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("price"))));
					activityObj.put("rating", Bytes.toInt(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("rating"))));
					activityObj.put("time", Bytes.toString(result.getValue(Bytes.toBytes("info"),Bytes.toBytes("time"))));
					
			    	jsonTxt = activityObj.toJSONString();
			    	
			    	if (StringUtil.isNotEmpty(jsonTxt)) 
			    	{
			    		activityTO = new ActivityTO(jsonTxt); 
			    	}
				}

				activityTable.close();
				System.out.println("getMovieRating success!");

			} //if (movieId > 0 )
		} catch (Exception e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
        return activityTO;
    } //getMovieRating 
    
    /*public static void putCustomerGenreMovieTable()
    {
    	try {
			Table customerGenreMovieTable = getTable(TABLE_NAME + "." + CUSTOMER_GENRE_MOVIE_TABLE);
			File file = new File("F:\\customerGenreMovie2.txt");//Text文件
			BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
			String s = null;
			int id = 0;
			int genreId = 0;
			int movieId =0 ;
			while((s = br.readLine())!=null){//使用readLine方法，一次读一行
				if(s.indexOf("--More--")!=-1)
	    		{System.out.println(s);}
	    		else if(s.isEmpty())
	    		{}
	    		else
	    		{
					JSONObject obj = JSONObject.parseObject(s);
					id = obj.getIntValue("id");
					genreId = obj.getIntValue("genreId");
					movieId = obj.getIntValue("movieId");
					Put put = new Put(Bytes.toBytes(id+"_"+genreId+"_"+movieId));
					put.addColumn(Bytes.toBytes("customerGenre"), Bytes.toBytes("id"), Bytes.toBytes(id));
					put.addColumn(Bytes.toBytes("customerGenre"), Bytes.toBytes("genreId"), Bytes.toBytes(genreId));
					put.addColumn(Bytes.toBytes("customerGenre"), Bytes.toBytes("movieId"), Bytes.toBytes(movieId));
					customerGenreMovieTable.put(put);
	    		}
			}
			br.close();
			customerGenreMovieTable.close();
			System.out.println("导入成功！！！");
		} catch (FileNotFoundException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}
    }*/
    
    /*public static void putCustomerGenresTable() throws Exception
    {
    	Table customerGenresTable = getTable(TABLE_NAME + "." + CHILD_TABLE);
    	File file = new File("F:\\customerGenres.txt");//Text文件
    	BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
    	String s = null;
    	while((s = br.readLine())!=null){//使用readLine方法，一次读一行
    		JSONObject obj = JSONObject.parseObject(s);
    		JSONArray arr = obj.getJSONArray("genres");
    		String str = JSONArray.toJSONString(arr);
    		Put put = new Put(Bytes.toBytes(obj.getIntValue("id")));
    		put.addColumn(Bytes.toBytes("customerGenre"), Bytes.toBytes("id"), Bytes.toBytes(obj.getIntValue("id")));
    		put.addColumn(Bytes.toBytes("customerGenre"), Bytes.toBytes("cid"), Bytes.toBytes(obj.getIntValue("cid")));
    		put.addColumn(Bytes.toBytes("genres"), Bytes.toBytes("genres_list"), Bytes.toBytes(str));
    		customerGenresTable.put(put);
    	}
    	br.close();
    	customerGenresTable.close();
    }*/
}
