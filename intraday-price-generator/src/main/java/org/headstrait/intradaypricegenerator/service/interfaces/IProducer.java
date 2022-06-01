package org.headstrait.intradaypricegenerator.service.interfaces;


public interface IProducer<K,V> {
    /**
     *
     * @param key of the event to be streamed
     * @param value of the event to be streamed
     */
    void send(K key, V value);

    /**
     * shuts down the kafka producer after use
     */
    void shutdown();
}
