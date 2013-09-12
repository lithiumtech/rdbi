package com.lithium.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lithium.rdbi.RedisResultMapper;
import org.joda.time.Instant;

import java.util.List;

public class JobInfoListMapper implements RedisResultMapper<List<JobInfo>> {

    @Override
    public List<JobInfo> map(Object result) {

        List<String> results = (List<String>) result;
        List<JobInfo> infos = Lists.newArrayList();

        for (int i = 0; i < results.size(); i = i + 2) {
            infos.add(new JobInfo(results.get(i), new Instant(Long.valueOf(results.get(i+1)))));
        }

        return infos;
    }
}
