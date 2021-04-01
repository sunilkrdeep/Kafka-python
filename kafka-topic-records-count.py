import datetime
import os


def kafka_topic_msgCount(brokers, topicName):
    connection = "kafka-run-class kafka.tools.GetOffsetShell --broker-list" + brokers \
                 + "--topic " + topicName
    # print(connection)

    timestamp = datetime.datetime.now().isoformat()
    msgrecordsfull = os.popen(connection).readlines()

    msgcount = 0
    for i in range(len(msgrecordsfull)):
        #print(msgrecordsfull[i].split(":")[2])
        msgcount = msgcount + int(msgrecordsfull[i].split(":")[2])

    tempList = [timestamp, msgrecordsfull[0].split(":")[0], msgcount]
    print(tempList)


if __name__ == '__main__':
    brokers = " 192.168.56.103:9092 "
    topic = input('Enter Topic Name with quotes <"***-***-**">: ')
    kafka_topic_msgCount(brokers, topic)
