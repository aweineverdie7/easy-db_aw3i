package controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import dto.ActionDTO;
import dto.ActionTypeEnum;
import dto.RespDTO;
import dto.RespStatusTypeEnum;
import service.DataService;
import service.Store;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/easydb")
public class DBServlet extends HttpServlet {

    private final DataService dataService = new DataService(); // 实例化数据服务

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        processRequest(req, resp, ActionTypeEnum.GET);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
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
        String key;
        String value = null;

        if ("POST".equalsIgnoreCase(req.getMethod())) {
            // 使用Fastjson解析POST请求的请求体
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(req.getInputStream()))) {
                StringBuilder requestBody = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    requestBody.append(line);
                }
                Map<String, String> requestBodyMap = JSON.parseObject(requestBody.toString(), new TypeReference<Map<String, String>>(){});
                key = requestBodyMap.get("key");
                value = requestBodyMap.get("value");
            } catch (Exception e) {
                // JSON解析失败处理
                handleException(resp, "Invalid JSON request body", e);
                return;
            }
        }  else {
            // GET请求或其他，继续使用getParameter获取参数
            key = req.getParameter("key");
        }

        // 参数验证
        if (key == null || key.trim().isEmpty()) {
            handleException(resp, "Missing or invalid 'key' parameter");
            return;
        }

        ActionDTO action = new ActionDTO(actionType, key, value);

        RespDTO response = dataService.handleAction(action);

        resp.setContentType("application/json");
        PrintWriter out = resp.getWriter();
        out.print(response.toJson());
        out.flush();
    }

    // 异常处理方法，用于设置错误响应
    private void handleException(HttpServletResponse resp, String errorMessage, Exception... cause) throws IOException {
        RespDTO errorResponse = new RespDTO(RespStatusTypeEnum.FAIL, errorMessage);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        resp.getWriter().print(errorResponse.toJson());
        if (cause.length > 0 && cause[0] != null) {
            cause[0].printStackTrace();
        }
    }
}
