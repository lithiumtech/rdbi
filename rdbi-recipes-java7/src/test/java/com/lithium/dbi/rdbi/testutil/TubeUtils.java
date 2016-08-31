package com.lithium.dbi.rdbi.testutil;

import java.util.UUID;

public class TubeUtils {

    public static String uniqueTubeName() {
        return "test_tube_" + UUID.randomUUID().toString();
    }

}
