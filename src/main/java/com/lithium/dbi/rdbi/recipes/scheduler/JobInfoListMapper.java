package com.lithium.dbi.rdbi.recipes.scheduler;

import com.google.common.collect.Lists;
import com.lithium.dbi.rdbi.ResultMapper;

import java.util.List;

public class JobInfoListMapper implements ResultMapper<List<JobInfo>, List<String>> {

    @Override
    public List<JobInfo> map(List<String> results) {
        List<JobInfo> infos = Lists.newArrayList();   //CR: Use .newLinkedList or .newArrayListWithCapacity(results.size() / 2)

        for (int i = 0; i < results.size(); i = i + 2) {
            infos.add(new JobInfo(results.get(i), Double.valueOf(results.get(i+1))));
        }

        return infos;
    }
}
