package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.sql.Timestamp;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {
    /**
     * Rate limiter. De ventana deslizante.
     * Implementado con un conjunto ordenado (sorted set)
     * Cada entrada del conjunto tendrá como [Score] el milisegundo en que se almacenó.
     * Para de ésta forma poder borrar entradas menores a la ventana y contar cuantas hay actualmente.
     * La clave de cada entrada debe identificar el origen de la petición y ser única en cada momento
     * Por ejemplo : [timestamp]-[ip] o [timestamp]-[random]
     */
    private final JedisPool jedisPool;
    // Longitud de la ventana en milisegundos
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        Long millis = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String key = KeyHelper.getKey("limiter:" + windowSizeMS + ":" + name + ":" + maxHits);

        Response<Long> borrados;
        Response<Long> cuantos;

        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction tx = jedis.multi();

            tx.zadd(key,millis.doubleValue(),millis.toString());
            cuantos= tx.zcard(key);
            borrados = tx.zremrangeByScore(key,0,millis-windowSizeMS);

            tx.exec();
        }
        System.out.print(" Cuantos hay " + cuantos.get());
        System.out.println(" Borrado " + borrados.get() );
        if ( cuantos.get() >= maxHits) {
            System.out.println(" Exception " );
            throw new RateLimitExceededException();
        }
        // END CHALLENGE #7
    }
}
