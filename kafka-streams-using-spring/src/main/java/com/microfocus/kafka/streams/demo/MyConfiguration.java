package com.microfocus.kafka.streams.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Configuration
public class MyConfiguration
{
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs()
    {
        Map<String,Object> props = new HashMap<>();
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreamsConfiguration(props);
    }
    
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME)
    public StreamsBuilderFactoryBean streamsBuilderFactoryBean(KafkaStreamsConfiguration kafkaStreamsConfiguration)
    {
        return new StreamsBuilderFactoryBean(kafkaStreamsConfiguration);
    }
    
    @Bean
    public KStream<String,String> kafkaStreamWhichCopiedValueToKeyAndSetsValueAsNull(StreamsBuilder streamsBuilder)
    {
        KStream<String,String> stream = streamsBuilder.stream("kafka-streams-demo-input-topic");
        stream.selectKey(new KeyValueMapper<String,String,String>()
        {
            @Override
            public String apply(String key, String value)
            {
                return value;
            }
        }).mapValues(new ValueMapper<String,Object>()
        {
            @Override
            public Object apply(String value)
            {
                //set new value as null
                return null;
            }
        }).to("kafka-streams-demo-output-topic");
        return stream;
    }
}
