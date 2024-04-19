from confluent_kafka import Producer, Consumer
import json


# path="/usr/local/kafka/appKafka/intents.json"
# msg = read_file(path)
# print("msg:",msg)



class kafakapp:
    def __init__(self, config):
            self.config = config
            self.producer = Producer(self.config)
            self.consumer = Consumer(self.config)
        

    def sendMessage(self,topic,message):
            self.producer.produce(topic,message.encode())
            self.producer.flush()

    def consumerMessage(self,topic):
            self.consumer.subscribe([topic])
                
            while True:
                    message=self.consumer.poll(1.0)
                    if message is None:
                        continue
                    if message.error():
                            print("Consumer error:", message.error())
                            continue
                    data=json.loads(message.value().decode())
                    print("data tyep",type(data))
                    print("Received Message :",json.dumps(data))
                    self.sendMessage("target",json.dumps(data))
                    #self.targetMessage("target")

    # def targetMessage(self,targetTopic):
    #         self.consumer.subscribe([targetTopic])
                
    #         while True:
    #                 message=self.consumer.poll(1.0)
    #                 if message is None:
    #                     continue
    #                 if message.error():
    #                     print("Consumer error:", message.error())
    #                     continue
    #                 data=json.loads(message.value().decode())
    #                 print("data tyep",type(data))
    #                 print("Target Message :",json.dumps(data))

if __name__ == "__main__":
    config={
        'bootstrap.servers':'localhost:9092',
        'group.id':'my-group-new',
        'auto.offset.reset': 'earliest'
          }
    app=kafakapp(config)

#     msg = read_file("/usr/local/kafka/appKafka/intents.json")
#     json.dumps(msg)
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
    #app.sendMessage('first',msg)

    app.consumerMessage('source')
