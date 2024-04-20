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
                     d=(message.value().decode())
                    #print("d:",type(d))
                    #str to dict
                    print("Received Message :",d)
                    
                    self.sendMessage("target01",d)
                    #self.targetMessage("target")

    

if __name__ == "__main__":
    config={
        'bootstrap.servers':'localhost:9092',
        'group.id':'my-group-new',
        'auto.offset.reset': 'earliest'
          }
    app=kafakapp(config)


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
