package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper
{
    private static final Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    // ---------------------- helper ----------------------

    private static final JobScheduleHelper instance = new JobScheduleHelper();

    public static JobScheduleHelper getInstance()
    {
        return instance;
    }

    // ---------------------- core ----------------------

    public static final long PRE_READ_MS = 5000;    // pre-read 5 seconds

    private          Thread  scheduleThread;
    private          Thread  ringThread;
    private volatile boolean scheduleThreadToStop = false;
    private volatile boolean ringThreadToStop     = false;

    private static final Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();

    public void start()
    {
        /*
         * schedule thread
         * [1] 加行级锁
         * [2] 预读
         * [3] 推送至时间轮
         *     [3.1] 过期 > 5s：跳过 OR 立即触发 -> 生成下个触发点
         *     [3.2] 过期 < 5s：直接触发 + 生成下个触发点 -> 下个触发时间在当前 5 秒内则推送至时间轮 + 生成下个触发点
         *     [3.3] 推送至时间轮 -> 生成下个触发点
         * [4] 更新触发信息
         * [5] 关闭资源
         * [6] 预读成功 > 每秒扫描一次; 预读失败 > 跳过这个周期;
         */
        scheduleThread = new Thread(
                () ->
                {
                    try
                    {
                        // 以 5 秒为周期, 对齐到秒
                        TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
                    }
                    catch (InterruptedException e)
                    {
                        if (!scheduleThreadToStop)
                        {
                            logger.error(e.getMessage(), e);
                        }
                    }
                    logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

                    // pre-read count: treadPool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
                    int preReadCount = (
                            XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()
                    ) * 20;

                    while (!scheduleThreadToStop)
                    {
                        // Scan Job
                        long start = System.currentTimeMillis();

                        Connection        conn              = null;
                        Boolean           connAutoCommit    = null;
                        PreparedStatement preparedStatement = null;

                        boolean preReadSuc = true;
                        try
                        {
                            conn           = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
                            connAutoCommit = conn.getAutoCommit();
                            conn.setAutoCommit(false);

                            // 数据库锁
                            preparedStatement = conn.prepareStatement("select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
                            preparedStatement.execute();

                            // tx start

                            // 1、pre read 预读取
                            long             nowTime      = System.currentTimeMillis();
                            List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
                            if (scheduleList != null && !scheduleList.isEmpty())
                            {
                                // 2、push time-ring 向时间轮推送
                                for (XxlJobInfo jobInfo : scheduleList)
                                {
                                    // time-ring jump
                                    if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS)
                                    {
                                        // 2.1、trigger-expire > 5s：pass && make next-trigger-time
                                        // 过期 > 5s：跳过 && 生成下一个触发时间
                                        logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = {}", jobInfo.getId());

                                        // 1、misfire match
                                        MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
                                        if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum)
                                        {
                                            // FIRE_ONCE_NOW > trigger 立即触发
                                            JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
                                            logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = {}", jobInfo.getId());
                                        }

                                        // 2、fresh next
                                        refreshNextValidTime(jobInfo, new Date());
                                    }
                                    else if (nowTime > jobInfo.getTriggerNextTime())
                                    {
                                        // 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time
                                        // 过期 < 5s：直接触发 && 生成下一个触发时间

                                        // 1、trigger
                                        JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
                                        logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = {}", jobInfo.getId());

                                        // 2、fresh next
                                        refreshNextValidTime(jobInfo, new Date());

                                        // next-trigger-time in 5s, pre-read again 下个触发时间在当前 5 秒内
                                        if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime())
                                        {
                                            // 1、make ring second
                                            int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                            // 2、push time ring
                                            pushTimeRing(ringSecond, jobInfo.getId());

                                            // 3、fresh next
                                            refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                        }
                                    }
                                    else
                                    {
                                        // 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time
                                        // 未过期：时间轮触发 && 生成下一个触发时间

                                        // 1、make ring second
                                        int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

                                        // 2、push time ring
                                        pushTimeRing(ringSecond, jobInfo.getId());

                                        // 3、fresh next
                                        refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));
                                    }
                                }

                                // 3、update trigger info
                                for (XxlJobInfo jobInfo : scheduleList)
                                {
                                    XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
                                }
                            }
                            else
                            {
                                preReadSuc = false;
                            }

                            // tx stop
                        }
                        catch (Exception e)
                        {
                            if (!scheduleThreadToStop)
                            {
                                logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error: ", e);
                            }
                        }
                        finally
                        {
                            // commit
                            if (conn != null)
                            {
                                try
                                {
                                    // 1、提交事务
                                    conn.commit();
                                }
                                catch (SQLException e)
                                {
                                    if (!scheduleThreadToStop)
                                    {
                                        logger.error(e.getMessage(), e);
                                    }
                                }
                                try
                                {
                                    // 2、恢复自动提交
                                    conn.setAutoCommit(connAutoCommit);
                                }
                                catch (SQLException e)
                                {
                                    if (!scheduleThreadToStop)
                                    {
                                        logger.error(e.getMessage(), e);
                                    }
                                }
                                try
                                {
                                    // 3、关闭连接
                                    conn.close();
                                }
                                catch (SQLException e)
                                {
                                    if (!scheduleThreadToStop)
                                    {
                                        logger.error(e.getMessage(), e);
                                    }
                                }
                            }

                            // close PreparedStatement
                            if (null != preparedStatement)
                            {
                                try
                                {
                                    // 4、关闭 PreparedStatement
                                    preparedStatement.close();
                                }
                                catch (SQLException e)
                                {
                                    if (!scheduleThreadToStop)
                                    {
                                        logger.error(e.getMessage(), e);
                                    }
                                }
                            }
                        }

                        // 记录耗时
                        long cost = System.currentTimeMillis() - start;

                        // Wait seconds, align second
                        // 预期扫描时间小于 1 秒, 等待到 1 秒, 对齐到秒
                        if (cost < 1000)
                        {  
                            // scan-overtime, not wait
                            try
                            {
                                // pre-read period: success > scan each second; fail > skip this period;
                                // 预读成功 > 每秒扫描一次; 预读失败 > 跳过这个周期;
                                TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
                            }
                            catch (InterruptedException e)
                            {
                                if (!scheduleThreadToStop)
                                {
                                    logger.error(e.getMessage(), e);
                                }
                            }
                        }
                    }

                    logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
                }
        );
        scheduleThread.setDaemon(true);
        scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
        scheduleThread.start();

        // ring thread
        // 从时间轮中获取秒数据, 触发执行
        ringThread = new Thread(
                () ->
                {
                    while (!ringThreadToStop)
                    {
                        // align second 对齐到秒
                        try
                        {
                            TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
                        }
                        catch (InterruptedException e)
                        {
                            if (!ringThreadToStop)
                            {
                                logger.error(e.getMessage(), e);
                            }
                        }

                        try
                        {
                            // second data
                            List<Integer> ringItemData = new ArrayList<>();
                            int           nowSecond    = Calendar.getInstance().get(Calendar.SECOND); // 避免处理耗时太长, 跨过刻度, 向前校验一个刻度；
                            for (int i = 0; i < 2; i++)
                            {
                                List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
                                if (tmpData != null)
                                {
                                    ringItemData.addAll(tmpData);
                                }
                            }

                            // ring trigger
                            logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : {} = {}", nowSecond, Collections.singletonList(ringItemData));
                            if (!ringItemData.isEmpty())
                            {
                                // do trigger
                                for (int jobId : ringItemData)
                                {
                                    // do trigger
                                    JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
                                }
                                // clear
                                ringItemData.clear();
                            }
                        }
                        catch (Exception e)
                        {
                            if (!ringThreadToStop)
                            {
                                logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error: ", e);
                            }
                        }
                    }
                    logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
                }
        );
        ringThread.setDaemon(true);
        ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
        ringThread.start();
    }

    private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime)
    {
        try
        {
            // nextValidTime 不可能为 null, 因为是通过时间获取任务的
            Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
            if (nextValidTime != null)
            {
                jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
                jobInfo.setTriggerNextTime(nextValidTime.getTime());
            }
            else
            {
                // generateNextValidTime fail, stop job
                jobInfo.setTriggerStatus(0);
                jobInfo.setTriggerLastTime(0);
                jobInfo.setTriggerNextTime(0);
                logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
                        jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
            }
        }
        catch (Exception e)
        {
            // generateNextValidTime error, stop job
            jobInfo.setTriggerStatus(0);
            jobInfo.setTriggerLastTime(0);
            jobInfo.setTriggerNextTime(0);
            logger.error(">>>>>>>>>>> xxl-job, refreshNextValidTime error for job: jobId={}, scheduleType={}, scheduleConf={}",
                    jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf(), e);
        }
    }

    private void pushTimeRing(int ringSecond, int jobId)
    {
        // push async ring
        List<Integer> ringItemData = ringData.computeIfAbsent(ringSecond, k -> new ArrayList<>());
        ringItemData.add(jobId);

        logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : {} = {}", ringSecond, Collections.singletonList(ringItemData));
    }

    public void toStop()
    {
        // 1、stop schedule
        scheduleThreadToStop = true;
        try
        {
            TimeUnit.SECONDS.sleep(1);  // wait
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
        if (scheduleThread.getState() != Thread.State.TERMINATED)
        {
            // interrupt and wait
            scheduleThread.interrupt();
            try
            {
                scheduleThread.join();
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
            }
        }

        // if has ring data
        boolean hasRingData = false;
        if (!ringData.isEmpty())
        {
            for (int second : ringData.keySet())
            {
                List<Integer> tmpData = ringData.get(second);
                if (tmpData != null && !tmpData.isEmpty())
                {
                    hasRingData = true;
                    break;
                }
            }
        }
        if (hasRingData)
        {
            try
            {
                TimeUnit.SECONDS.sleep(8);
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
            }
        }

        // stop ring (wait job-in-memory stop)
        ringThreadToStop = true;
        try
        {
            TimeUnit.SECONDS.sleep(1);
        }
        catch (InterruptedException e)
        {
            logger.error(e.getMessage(), e);
        }
        if (ringThread.getState() != Thread.State.TERMINATED)
        {
            // interrupt and wait
            ringThread.interrupt();
            try
            {
                ringThread.join();
            }
            catch (InterruptedException e)
            {
                logger.error(e.getMessage(), e);
            }
        }

        logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
    }

    // ---------------------- tools ----------------------

    public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception
    {
        ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
        if (ScheduleTypeEnum.CRON == scheduleTypeEnum)
        {
            return new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
        }
        else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/)
        {
            return new Date(fromTime.getTime() + Integer.parseInt(jobInfo.getScheduleConf()) * 1000L);
        }
        return null;
    }
}
