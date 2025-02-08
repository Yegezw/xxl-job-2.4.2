package com.xxl.job.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * net util
 *
 * @author xuxueli 2017-11-29 17:00:25
 */
public class NetUtil
{
    private static final Logger logger = LoggerFactory.getLogger(NetUtil.class);

    /**
     * find avaliable port
     *
     * @param defaultPort
     * @return
     */
    public static int findAvailablePort(int defaultPort)
    {
        int portTmp = defaultPort;
        while (portTmp < 65535)
        {
            if (isPortNotUsed(portTmp))
            {
                return portTmp;
            }
            else
            {
                portTmp++;
            }
        }
        portTmp = --defaultPort;
        while (portTmp > 0)
        {
            if (isPortNotUsed(portTmp))
            {
                return portTmp;
            }
            else
            {
                portTmp--;
            }
        }
        throw new RuntimeException("no available port.");
    }

    /**
     * check port not used
     *
     * @param port 端口
     * @return 未被使用 ? true : false
     */
    public static boolean isPortNotUsed(int port)
    {
        boolean      unused       = true;
        ServerSocket serverSocket = null;
        try
        {
            serverSocket = new ServerSocket(port);
        }
        catch (IOException e)
        {
            logger.info(">>>>>>>>>>> xxl-job, port[{}] is in use.", port);
            unused = false;
        }
        finally
        {
            if (serverSocket != null)
            {
                try
                {
                    serverSocket.close();
                }
                catch (IOException e)
                {
                    logger.info("");
                }
            }
        }
        return unused;
    }
}
