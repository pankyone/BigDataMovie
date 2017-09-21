package oracle.demo.oow.bd.ui;

import java.io.IOException;
import java.util.Date;
import javax.servlet.*;
import javax.servlet.http.*;

import oracle.demo.oow.bd.dao.CustomerRatingDAO;
import oracle.demo.oow.bd.dao.HadoopActivityDAO;
import oracle.demo.oow.bd.dao.HadoopCustomerDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;

public class logIn extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private String loginPage = "login.jsp";
    private String indexPage = "index.jsp";

    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    public void doGet(HttpServletRequest request,HttpServletResponse response) 
    		throws ServletException,IOException {
        doPost(request, response);
    }

    public void doPost(HttpServletRequest request,HttpServletResponse response) 
    		throws ServletException,IOException {
        response.getWriter();

        try {
                String username = request.getParameter("username");
                String password = request.getParameter("password");
                boolean useMoviePosters = request.getParameter("useMoviePosters") == null? false : true;
                
                CustomerRatingDAO custRatingDAO = new CustomerRatingDAO();
                HadoopCustomerDAO cdao = new HadoopCustomerDAO();
                CustomerTO cto =
                    cdao.getCustomerByCredential(username, password);
                Date date = new Date();

                if (cto != null) {

                    // Delete all the previous ratings of the customers from the DB
                    custRatingDAO.deleteCustomerRating(cto.getId());
                    
                    /////// ACTIVITY ////////
                    ActivityTO activityTO = new ActivityTO();
                    activityTO.setActivity(ActivityType.LOGIN);
                    activityTO.setCustId(cto.getId());
                    HadoopActivityDAO aDAO = new HadoopActivityDAO();
                    aDAO.insertCustomerActivity(activityTO);
                    
                    /*activityTO.setActivity(ActivityType.LIST_MOVIES);
                    aDAO.insertCustomerActivity(activityTO);*/
                    
                    HttpSession session = request.getSession();
                    session.setAttribute("username", username);
                    session.setAttribute("time", date);
                    session.setAttribute("userId", cto.getId());
                    session.setAttribute("name", cto.getName());
                    session.setAttribute("useMoviePosters", useMoviePosters);
                    //Ashok
                    System.out.println("login success!");
                    response.sendRedirect(indexPage);
                    
                } else {
                    response.sendRedirect(loginPage + "?error=1");
                }


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
