#!/usr/bin/python3
import boto3
import json
import sys
import time
import argparse
import colorama
from termcolor import colored
from config import config
from datetime import datetime

colorama.init()
global successFile
successFile = ''
global intSuccessMatches
intSuccessMatches = 0

class VideoDetect:
  jobId = ''
  rek = boto3.client('rekognition')
  sqs = boto3.client('sqs')
  sns = boto3.client('sns')
    
  roleArn = ''
  bucket = ''
  video = ''
  startJobId = ''

  sqsQueueUrl = ''
  snsTopicArn = ''
  processType = ''

  def __init__(self, role, bucket, video):    
    self.roleArn = role
    self.bucket = bucket
    self.video = video

  def GetSQSMessageSuccess(self):
    jobFound = False
    succeeded = False
    dotLine=0
    while (jobFound == False):
      sqsResponse = self.sqs.receive_message(QueueUrl=self.sqsQueueUrl, MessageAttributeNames=['ALL'], MaxNumberOfMessages=10)
      if (sqsResponse):      
        if ('Messages' not in sqsResponse):
            if (dotLine < 40):
                print('.', end='')
                dotLine = dotLine + 1
            else:
                print()
                dotLine = 0    
            sys.stdout.flush()
            time.sleep(5)
            continue

        for message in sqsResponse['Messages']:
            notification = json.loads(message['Body'])
            rekMessage = json.loads(notification['Message'])
            if (rekMessage['Status'] == 'FAILED'):
              print(colored('\nERROR Message Below:\n {}'.format(rekMessage), 'red'))
              exit()
            print(rekMessage['JobId'])
            print(rekMessage['Status'])
            if (rekMessage['JobId'] == self.startJobId):
                print('Matching Job Found:' + rekMessage['JobId'])
                jobFound = True
                if (rekMessage['Status']=='SUCCEEDED'):
                    succeeded=True

                self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                                ReceiptHandle=message['ReceiptHandle'])
            else:
                print("Job didn't match:" +
                        str(rekMessage['JobId']) + ' : ' + self.startJobId)
            # Delete the unknown message. Consider sending to dead letter queue
            self.sqs.delete_message(QueueUrl=self.sqsQueueUrl,
                            ReceiptHandle=message['ReceiptHandle'])

        return succeeded

  def StartFaceSearchCollection(self, collection):
    response = self.rek.start_face_search(Video={'S3Object':{'Bucket':self.bucket,'Name':self.video}},
        CollectionId=collection,
        NotificationChannel={'RoleArn':self.roleArn, 'SNSTopicArn':self.snsTopicArn})  
    self.startJobId = response['JobId']  
    print('Start Job ID: {}'.format(self.startJobId))


  def GetFaceSearchCollectionResults(self, collection, maxFaces):
    maxResults = maxFaces
    paginationToken = ''
    finished = False

    while (finished == False):
      response = self.rek.get_face_search(JobId=self.startJobId,
                                          MaxResults=maxResults,
                                          NextToken=paginationToken)
      #print(str(response['VideoMetadata']['DurationMillis']))
      for personMatch in response['Persons']:
        if ('FaceMatches' in personMatch):
          for faceMatch in personMatch['FaceMatches']:
            global intSuccessMatches
            intSuccessMatches += 1
            print(colored('Person Found. Timestamp : {} milliseconds'.format(str(personMatch['Timestamp']))))
            print(colored('Face from Collection {} & {} are of the same person, with similarity: {}\n'.format(collection, self.video, faceMatch['Similarity']), 'green'))
            successFile.write('Person Found. Timestamp : {} milliseconds\n'.format(str(personMatch['Timestamp'])))
            successFile.write('Face from Collection {} & {} are of the same person, with similarity: {}\n'.format(collection, self.video, faceMatch['Similarity']))
            successFile.flush()
        if ('NextToken' in response):
          paginationToken = response['NextToken']
        else:
          finished = True

  def CreateTopicandQueue(self):
    millis = str(int(round(time.time() * 1000)))
    #Create SNS topic
    snsTopicName = "AmazonRekognitionExample" + millis
    topicResponse = self.sns.create_topic(Name=snsTopicName)
    self.snsTopicArn = topicResponse['TopicArn']

    #create SQS queue
    sqsQueueName = "AmazonRekognitionQueue" + millis
    self.sqs.create_queue(QueueName=sqsQueueName)
    self.sqsQueueUrl = self.sqs.get_queue_url(QueueName=sqsQueueName)['QueueUrl']
    attribs = self.sqs.get_queue_attributes(QueueUrl=self.sqsQueueUrl, AttributeNames=['QueueArn'])['Attributes']
    sqsQueueArn = attribs['QueueArn']

    # Subscribe SQS queue to SNS topic
    self.sns.subscribe(TopicArn=self.snsTopicArn, Protocol='sqs', Endpoint=sqsQueueArn)

    #Authorize SNS to write SQS queue 
    policy = """{{
        "Version":"2012-10-17",
        "Statement":[
            {{
            "Sid":"MyPolicy",
            "Effect":"Allow",
            "Principal" : {{"AWS" : "*"}},
            "Action":"SQS:SendMessage",
            "Resource": "{}",
            "Condition":{{
                "ArnEquals":{{
                "aws:SourceArn": "{}"
                }}
            }}
            }}
        ]
    }}""".format(sqsQueueArn, self.snsTopicArn)
 
    self.sqs.set_queue_attributes(
        QueueUrl = self.sqsQueueUrl,
        Attributes = {
            'Policy' : policy
    })

  def DeleteTopicandQueue(self):
    self.sqs.delete_queue(QueueUrl=self.sqsQueueUrl)
    self.sns.delete_topic(TopicArn=self.snsTopicArn)

def getMinuteLanguage(intMinutes):
    if intMinutes == 1:
        return 'Minute'
    return 'Minutes'

def getSecondLanguage(intSeconds):
    if intSeconds == 1:
        return 'Second'
    return 'Seconds'

def argParse():
  parser = argparse.ArgumentParser(description='Find a face collection in video for AWS Rekognition.')
  parser.add_argument('collection_name', type=str,
                    help='name the face collection')
  parser.add_argument('-m', '--max-face', dest='max_face', type=int,
                    default=1000, help='Maximum amount of faces you want to find in each frame. Default is 1000')
  parser.add_argument('-b', '--bucket', dest='bucket', type=str,
                    default=config["s3_bucket_name"], help='S3 Bucket you want to pick. Default is in config.py')
  parser.add_argument('-v', '--video', dest='video', type=str,
                    default=config["video_name"], help='Video Name you want to analyze. Default is in config.py')
  parser.add_argument('-arn', '--role-arn', dest='role_arn', type=str,
                    default=config["role_arn"], help='Role ARN. Default is in config.py')
  parser.add_argument('-o', '--output-file', dest='output_file_name', type=str, default='success.txt',
                    help='Name of the output file. Default is success.txt')
  return parser.parse_args()

def main():
  args = argParse()
  global successFile
  successFile = open(args.output_file_name,'a')
  roleArn = args.role_arn
  bucket = args.bucket
  video = args.video
  collection = args.collection_name

  beforeProcess = datetime.now()
  print('Time Started: {}'.format(beforeProcess))
  analyzer = VideoDetect(roleArn, bucket,video)
  analyzer.CreateTopicandQueue()

  analyzer.StartFaceSearchCollection(collection)
  if (analyzer.GetSQSMessageSuccess() == True):
    analyzer.GetFaceSearchCollectionResults(collection, args.max_face)
    
  analyzer.DeleteTopicandQueue()
  difference = (datetime.now() - beforeProcess)
  seconds_in_day = 24 * 60 * 60
  timeElapsed = divmod(difference.days * seconds_in_day + difference.seconds, 60)
  print('Total Time Elapsed: {} {} {} {}'.format(timeElapsed[0], getMinuteLanguage(timeElapsed[0]), timeElapsed[1], getSecondLanguage(timeElapsed[1])))
  print('Total Faces Found: {}'.format(intSuccessMatches))


if __name__ == "__main__":
  main()