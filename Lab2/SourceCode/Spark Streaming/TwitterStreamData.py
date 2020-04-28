import os
import time

import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
consumer_key = 'gSEKhW3jUNeWSE7UDcx2FaGS5'
consumer_secret = 'bPlz8kEBJWsQqg6sVLuZaYcJO0b9WBBOFpxCJASwxk2TMdVO2E'
access_token = '1066068152-3qMacgRh4mYtA5WBA44ldHSyqO0qp25Vub4w9Ie'
access_secret = 'yYp4RGJDiQqh0XF2D6gBOopG3zaNcPlDpfmFAOZay3VMA'


# we create this class that inherits from the StreamListener in tweepy StreamListener
class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    # we override the on_data() function in StreamListener
    def on_data(self, data):
        # try:
            message = json.loads(data)
            # print(message['text'].encode('utf-8'))
            print(message['text'].encode('utf-8'))
            # c = message['text'] + "\n"
            self.client_socket.send(message['text'].encode('utf-8'))
            # time.sleep(30)
            return True
        # except BaseException as e:
        #     print("Error on_data: %s" % str(e))
        # return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['football'])  # this is the topic we are interested in


if __name__ == "__main__":
    new_skt = socket.socket()  # initiate a socket object
    host = "127.0.0.1"  # local machine address
    port = 8085  # specific port for your service.
    new_skt.bind((host, port))  # Binding host and port

    print("Now listening on port: %s" % str(port))

    new_skt.listen(5)  # waiting for client connection.
    c, addr = new_skt.accept()  # Establish connection with client. it returns first a socket object,c, and the address bound to the socket

    print("Received request from: " + str(addr))
    # c.send(b'Helo Hi Hi How Helo')
    # and after accepting the connection, we can send the tweets through the socket
    send_tweets(c)