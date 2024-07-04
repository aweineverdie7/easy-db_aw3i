package controller;

import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import service.DataService;
import service.Store;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class DBServlet extends HttpServlet {

    private final DataService dataService = new DataService(); // 实例化数据服务

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        processRequest(req, resp, ActionTypeEnum.GET);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // 假设POST用于设置数据，根据实际情况调整
        processRequest(req, resp, ActionTypeEnum.SET);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // PUT逻辑，根据业务需求实现
    }

    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        processRequest(req, resp, ActionTypeEnum.RM);
    }

    private void processRequest(HttpServletRequest req, HttpServletResponse resp, ActionTypeEnum actionType) throws IOException {
        // 从请求中构建ActionDTO（此处需根据请求参数实际构造）
        String key = req.getParameter("key");
        String value = req.getParameter("value");
        ActionDTO action = new ActionDTO(actionType,key,value);

        RespDTO response = dataService.handleAction(action);

        resp.setContentType("application/json"); // 设置响应内容类型
        PrintWriter out = resp.getWriter();
        out.print(response.toJson()); // 假设RespDTO有toJson方法来序列化为JSON字符串
        out.flush();
    }
}
