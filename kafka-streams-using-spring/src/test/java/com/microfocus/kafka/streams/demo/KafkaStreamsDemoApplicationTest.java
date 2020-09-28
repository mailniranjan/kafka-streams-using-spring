package com.microfocus.kafka.streams.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest
@RunWith(SpringRunner.class)
public class KafkaStreamsDemoApplicationTest
{
    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    
    @Autowired
    MyConfiguration myConfiguration;
    
    @Test
    public void testHappyPath() throws Exception
    {
        StreamsBuilder streamsBuilder = streamsBuilderFactoryBean.getObject();
        
        //need to find how spring for kafka does this. we shouldn't be doing this
        myConfiguration.kafkaStreamWhichCopiedValueToKeyAndSetsValueAsNull(streamsBuilder);
        
        Topology topology = streamsBuilder.build();
        
        // setup test driver
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        try(TopologyTestDriver testDriver = new TopologyTestDriver(topology, props))
        {
            ConsumerRecordFactory<String,String> factory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    
            String inputKey = "key";
            String inputValue = "value";
            
            //Act
            testDriver.pipeInput(factory.create(myConfiguration.getInputTopic(), inputKey, inputValue));
    
            ProducerRecord<String,String> outputRecord = testDriver.readOutput(myConfiguration.getOutputTopic(), new StringDeserializer(),
                    new StringDeserializer());
    
            //Assert
            String expectedKey = inputValue;
            String expectedValue = null;
            OutputVerifier.compareKeyValue(outputRecord, expectedKey, expectedValue);
        }
    }
}
