package io.chaosforge.controlplane.cache;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

/** Evicts the named cache's L1 entry on an invalidation message (ADR-0504). rule_sets never publish. */
public class CacheInvalidationListener implements MessageListener {

    private final Map<String, TwoLevelCache<?>> caches;

    public CacheInvalidationListener(Map<String, TwoLevelCache<?>> caches) {
        this.caches = caches;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), UTF_8);
        int idx = body.indexOf(CacheInvalidationPublisher.SEPARATOR);
        if (idx < 0) {
            return;
        }
        TwoLevelCache<?> cache = caches.get(body.substring(0, idx));
        if (cache != null) {
            cache.evictLocal(body.substring(idx + CacheInvalidationPublisher.SEPARATOR.length()));
        }
    }
}
