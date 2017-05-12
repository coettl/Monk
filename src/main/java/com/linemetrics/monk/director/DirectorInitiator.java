package com.linemetrics.monk.director;

import com.linemetrics.monk.config.dao.DirectorJob;
import com.linemetrics.monk.config.ConfigException;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@DisallowConcurrentExecution
public class DirectorInitiator implements Job {

    private static final Logger logger = LoggerFactory.getLogger(DirectorInitiator.class);

    DirectorJob                         job           = null;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        try {
            JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();

            if( ! dataMap.containsKey("job") ||
                ! (dataMap.get("job") instanceof Integer)) {
                return;
            }

            Integer jobId = (Integer)dataMap.get("job");
            job = Director.getDirectorJobById(jobId);

            logger.debug("Start Execution of Job-ID " + job.getId() + " with time range "
                    + (jobExecutionContext.getScheduledFireTime().getTime() - job.getDurationInMillis())
                    + " -> " + (jobExecutionContext.getScheduledFireTime().getTime()));

            if(job.getDurationSlice() == null) {

                RunnerContext ctx = new RunnerContext(
                        jobId,
                        jobExecutionContext.getScheduledFireTime().getTime() - job.getDurationInMillis(),
                        jobExecutionContext.getScheduledFireTime().getTime(),
                        job.getBatchSizeInMillis(),
                        job.getTimeZone()
                );

                DirectorRunner.getInstance().addContext(ctx);
            } else {

                long durationOffset = job.getDurationSliceInMillis();

                if(durationOffset <= 0) {
                    throw new ConfigException("Duration Slice (in Millis) for Job-ID " + job.getId() + " must be > 0");
                }

                logger.debug(" - Execute Slice for Job-ID " + job.getId() + " with duration "
                        + durationOffset);

                for(long startTime = jobExecutionContext.getScheduledFireTime().getTime() - job.getDurationInMillis();
                        startTime < (jobExecutionContext.getScheduledFireTime().getTime());
                        startTime += durationOffset) {

                    long endTime = startTime + durationOffset;
                    if(endTime > jobExecutionContext.getScheduledFireTime().getTime()) {
                        endTime = jobExecutionContext.getScheduledFireTime().getTime();
                    }

                    logger.debug(" - Execute Slice for Job-ID " + job.getId() + " with time range "
                            + startTime
                            + " -> " + endTime + " (" + (endTime - startTime) + ")");

                    RunnerContext ctx = new RunnerContext(
                            jobId,
                            startTime,
                            endTime,
                            job.getBatchSizeInMillis(),
                            job.getTimeZone()
                    );

                    DirectorRunner.getInstance().addContext(ctx);
                }

            }

        } catch(Exception exp) {
            exp.printStackTrace();
            throw new JobExecutionException(exp);
        }
    }
}
