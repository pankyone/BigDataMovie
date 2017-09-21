package oracle.demo.oow.bd.ui;

import java.io.IOException;
import javax.servlet.*;
import javax.servlet.http.*;
import oracle.demo.oow.bd.dao.HadoopActivityDAO;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.to.ActivityTO;

@SuppressWarnings("serial")
public class logout extends HttpServlet {

  private String loginPage = "login.jsp";

  public void init(ServletConfig config) throws ServletException {
    super.init(config);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doPost(request, response);
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    try {
      HttpSession session = request.getSession();
      if (!session.isNew()) {
          int userId = (Integer)request.getSession().getAttribute("userId");
          /////// ACTIVITY ////////
          ActivityTO activityTO = new ActivityTO();
          activityTO.setActivity(ActivityType.LOGOUT);
          activityTO.setCustId(userId);
          //activityTO.setPrice(1.99);
          HadoopActivityDAO aDAO = new HadoopActivityDAO();
          aDAO.insertCustomerActivity(activityTO);
        
        
          session.invalidate();
          session = request.getSession();
      }
      response.sendRedirect(loginPage);
    }catch (Exception e){
        response.sendRedirect(loginPage);
    }
  }
}
