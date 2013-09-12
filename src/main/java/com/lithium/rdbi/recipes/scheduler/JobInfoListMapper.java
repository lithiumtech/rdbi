package com.lithium.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.Instant;
import com.lithium.rdbi.ResultMapper;

import java.util.List;

public class JobInfoListMapper implements ResultMapper<List<JobInfo>> {

    @Override
    public List<JobInfo> map(Object result) {
        List<String> results = (List<String>) result; //CR: Unchecked cast
        List<JobInfo> infos = Lists.newArrayList();   //CR: Use .newLinkedList or .newArrayListWithCapacity(results.size() / 2)

        for (int i = 0; i < results.size(); i = i + 2) {
            infos.add(new JobInfo(results.get(i), new Instant(Long.valueOf(results.get(i+1)))));
        }

        return infos;
    }

}
