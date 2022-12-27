module rdbi.core {
    exports com.lithium.dbi.rdbi;
    requires net.bytebuddy;
    requires stringtemplate;
    requires io.opentelemetry.api;
    requires io.opentelemetry.context;
    requires redis.clients.jedis;
    requires jsr305;
    requires org.slf4j;
}
