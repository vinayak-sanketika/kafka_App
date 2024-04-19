from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import NewTopic,AdminClient
import json

admin_client = AdminClient({
     "bootstrap.servers": "localhost:9092"
     })

class kafakapp:
    def __init__(self, config):
            self.config = config
            self.producer = Producer(self.config)
            self.consumer = Consumer(self.config)
            
        
    def create_topic(self,source_topic,target_topic):
        topic_list = []
        topic_list.append(NewTopic(source_topic, 3, 1))
        topic_list.append(NewTopic(target_topic, 3, 1))
        admin_client.create_topics(topic_list)
        print("-------Created the topics--------")

        

    def sendMessage(self,topic,message):
            self.producer.produce(topic,message.encode())
            self.producer.flush()

            print(f"-----Message are sent to the {topic} topic------")

    def consumerMessage(self, sourceTopic, targetTopic):
        print("Inside the consumerMessage")
        self.consumer.subscribe([sourceTopic, targetTopic])
            
        while True:
            message = self.consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print("Consumer error:", message.error())
                continue
            #from obj to dict
            data = json.loads(message.value().decode())
            #print("Data type:", type(data))
            print("Received Message from Source:", json.dumps(data))
            
            
            if message.topic() == sourceTopic:
                #to sting from dict
                self.sendMessage(targetTopic, json.dumps(data))
            elif message.topic() == targetTopic:
                
                print("Received Message from Target:", json.dumps(data))

    
if __name__ == "__main__":
    config={
        'bootstrap.servers':'localhost:9092',
        'group.id':'my-group-new',
        #'auto.offset.reset': 'earliest'
          }
    app=kafakapp(config)
    app.create_topic('source','target')

    msg_json = "/usr/local/kafka/appKafka/data.json"
    json_list=[]
    with open (msg_json) as file:
        for jsonObj in file:
            jsonDict = json.loads(jsonObj)
            json_list.append(jsonDict)
    for item in json_list:
        # print(type(item))
        # print("item:",item)
        #to sting from dict
        items = json.dumps(item)
        # print(type(items))
        # print("items:",items)
        app.sendMessage('source',items)

    app.consumerMessage('source','target')
    