package com.lithium.dbi.rdbi.recipes.scheduler;

import com.lithium.dbi.rdbi.ResultMapper;

import java.util.ArrayList;
import java.util.List;

public class JobInfoListMapper implements ResultMapper<List<JobInfo>, List<String>> {

    @Override
    public List<JobInfo> map(List<String> results) {
        List<JobInfo> infos = new ArrayList<>();   //CR: Use .newLinkedList or .newArrayListWithCapacity(results.size() / 2)

        for (int i = 0; i < results.size(); i = i + 2) {
            infos.add(new JobInfo(results.get(i), Double.valueOf(results.get(i+1))));
        }

        return infos;
    }
}
