import ray 
import paho.mqtt.client as mqtt

ray.init()

# # Define the square task.
# @ray.remote
# def square(x):
#     return x * x

# # Launch four parallel square tasks.
# futures = [square.remote(i) for i in range(4)]

# # Retrieve results.
# print(ray.get(futures))


# # Define the Counter actor.
# @ray.remote
# class Counter:
#     def __init__(self):
#         self.i = 0

#     def get(self):
#         return self.i

#     def incr(self, value):
#         self.i += value

# # Create a Counter actor.
# c = Counter.remote()

# # Submit calls to the actor. These calls run asynchronously but in
# # submission order on the remote actor process.
# for _ in range(10):
#     c.incr.remote(1)

# # Retrieve final actor state.
# print(ray.get(c.get.remote()))
# # -> 10

# @ray.remote
# def factorial(n):
#     if n < 0:
#         return "Factorial is not defined for negative numbers"
#     elif n == 0:
#         return 1
#     else:
#         result = 1
#         for i in range(1, n + 1):
#             result *= i
#         return result
    

# print(ray.get(factorial.remote(4)))

# @ray.remote
# class Person:
#     def __init__(self, name, age):
#         self.name = name
#         self.age = age

#     def set_name(self, name):
#         self.name = name

#     def set_age(self, age):
#         self.age = age

#     def get_name(self):
#         return self.name

#     def get_age(self):
#         return self.age

# sam = Person.remote("Sam", 23)

# sam.set_age.remote(12)

# print(ray.get(sam.get_age.remote()))

@ray.remote
class MQTTClient:
    def __init__(self, broker_address, port, client_id=""):
        self.client = mqtt.Client(client_id)
        self.broker_address = broker_address
        self.port = port
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def connect(self):
        self.client.connect(self.broker_address, self.port)
        self.client.loop_start()

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def on_connect(self, mqtt_client, userdata, flags, rc):
        if rc == 0:
            print('Connected to broker successfully')
            mqtt_client.subscribe('#')
        else:
            print('Bad connection. Code:', rc)

    def on_message(self, mqtt_client, userdata, msg):
        print(f'Received message on topic: {msg.topic} with payload: {msg.payload}')

    def publish(self, topic, message):
        self.client.publish(topic, message)

    def subscribe(self, topic):
        self.client.subscribe(topic)


# Instantiate MQTTClient
mqtt_client = MQTTClient.remote("192.168.100.191", 1883, "ebuka")
# Connect to the broker
mqtt_client.connect.remote()
# Subscribe to a topic
mqtt_client.subscribe.remote("testTopicE")
# Publish a message to a topic
mqtt_client.publish.remote("testTopicE", "Hello, MQTT!")

while True:
    pass