package oracle.demo.oow.bd.dao;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.DBUtil;

/**
 * This class is used to access recommended movie data for customer
 */
public class CustomerRatingDAO extends DBUtil{
    
    private static Connection conn = null;

    public CustomerRatingDAO() {
        super();
        if (conn == null)
            conn =getConn();
    } //CustomerRatingDAO

    public void insertCustomerRating(int userId, int movieId, int rating) {
        String insert = null;
        PreparedStatement stmt = null;

        insert =
                "INSERT INTO CUSTRATING (custId, movieId, rating)  VALUES (?, ?, ?)";
        try {
            if (conn != null) {
                stmt = conn.prepareStatement(insert);
                stmt.setInt(1, userId);
                stmt.setInt(2, movieId);
                stmt.setInt(3, rating);
                stmt.execute();
                stmt.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteCustomerRating(int userId) {
        String delete = null;
        PreparedStatement stmt = null;

        delete = "DELETE FROM CUSTRATING WHERE custId = ?";
        try {
            if (conn != null) {
                stmt = conn.prepareStatement(delete);
                stmt.setInt(1, userId);
                stmt.execute();
                stmt.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public List<MovieTO> getMoviesByMood(int userId) {
    	
        List<MovieTO> movieList = null;
        String search = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        MovieTO movieTO = null;
        HadoopMovieDAO movieDAO = new HadoopMovieDAO();
        String title = null;
        Hashtable<String, String> movieHash = new Hashtable<String, String>();
        
        search = "select movieId from Recommend where custId = '" + userId + "' "
        		+ "And movieId Not In (Select movieId from CUSTRATING Where custId = '" + userId + "') order by scord DESC";
        try {
            if (conn != null) {
                //initialize movieList only when connection is successful
            	
                movieList = new ArrayList<MovieTO>();
                stmt = conn.prepareStatement(search);
                rs = stmt.executeQuery();
                while (rs.next()) {
                    //Retrieve by column name
                    int id = rs.getInt(1);
                    //create new object
                    movieTO = movieDAO.getMovieById(id);
                    if (movieTO != null) {
                        title = movieTO.getTitle();
                        //Make sure movie title doesn't exist before in the movieHash
                        if (!movieHash.containsKey(title)) {
                            movieHash.put(title, title);
                            movieList.add(movieTO);
                        }
                    } //if (movieTO != null)
                } //EOF while
            } //EOF if (conn!=null)
        } catch (Exception e) {
            e.printStackTrace();         
        }
        return movieList;
    }
}
