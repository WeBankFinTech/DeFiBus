package com.webank.defibus.hook;

import com.webank.defibus.common.DeFiBusConstant;
import java.util.HashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ClientRPCHook implements RPCHook {
    private RPCHook rpcHook;
    private String clientId;

    public ClientRPCHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        setRequestExtInfo(request);
        if (rpcHook != null) {
            rpcHook.doBeforeRequest(remoteAddr, request);
        }
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
    }

    private void setRequestExtInfo(RemotingCommand request) {
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields == null) {
            extFields = new HashMap<>();
            request.setExtFields(extFields);
        }
        int code = request.getCode();
        switch (code) {
            case RequestCode.PULL_MESSAGE: {
                if (StringUtils.isNotBlank(clientId)) {
                    extFields.put(DeFiBusConstant.CLIENT_ID, clientId);
                }
                break;
            }
            default:
                break;
        }
    }
}