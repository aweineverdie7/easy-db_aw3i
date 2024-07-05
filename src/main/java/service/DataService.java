package service;

import dto.ActionDTO;
import dto.RespDTO;
import dto.RespStatusTypeEnum;

import java.io.File;

public class DataService {
    private NormalStore store;
    public DataService() {
        String dataDir = "severlet" + File.separator;
        store = new NormalStore(dataDir);
    }


    public RespDTO handleAction(ActionDTO action) {
        switch (action.getType()) {
            case GET:
                return new RespDTO(RespStatusTypeEnum.SUCCESS, store.get(action.getKey()));
            case SET:
                store.set(action.getKey(), action.getValue());
                return new RespDTO(RespStatusTypeEnum.SUCCESS, null);
            case RM:
                store.rm(action.getKey());
                return new RespDTO(RespStatusTypeEnum.SUCCESS, null);
            // 其他命令处理...
            default:
                return new RespDTO(RespStatusTypeEnum.FAIL, "未知的命令类型");
        }
    }
}
