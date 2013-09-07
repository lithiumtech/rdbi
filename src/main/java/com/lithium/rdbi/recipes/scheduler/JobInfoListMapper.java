package com.lithium.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lithium.rdbi.RedisResultMapper;

import java.util.List;

public class JobInfoListMapper implements RedisResultMapper<List<JobInfo>> {

    @Override
    public List<JobInfo> map(Object result) {

        List<String> results = (List<String>) result;
        List<JobInfo> infos = Lists.newArrayList();

        for (String res : results) {
            infos.add(new JobInfo(JobState.TIMEDOUT, res, null, null));
        }

        return infos;
    }
}
