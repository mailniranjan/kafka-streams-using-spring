# kafka-streams-using-spring
Experiment kafka streams functionality using spring for kafka streams

Here I will poll a record from one topic in kafka and do some processing
and put in another topic in kafka. The target record will have value
null and will have key set to the value from input topic.

For ex: input topic has record key and value like (null, value). Record
from input topic can have any key which is ignored in my logic.

I will put the record (value, null) to output topic.

The input and output topic has to be created manually.
