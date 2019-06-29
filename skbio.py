#This is the bio lambda example.  The others are the same.
#to use it you must add the layer arn:aws:lambda:us-west-2:066301190734:layer:dbgscikit4:2
#which contains all the things you need below.  

import numpy as np
import pickle
import random
import json
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from socket import gethostname
import time
import boto3
import urllib
#we have loaded the restricted model into the library
from simple_model import Model

s3 = boto3.resource("s3")
#    aws_access_key_id = 'YOUR ACCESS KEY',
#    aws_secret_access_key = 'yourlongsecretkey' )


topic = 'bio'
mod = Model(s3, 'bio')


table_name = 'dbgtesttable'
table = boto3.resource('dynamodb').Table(table_name)
Lam = boto3.client("lambda",region_name="us-west-2")

def grabTopic(s):
    #print("s=",s)
    b = s.find('[')
    e = s.find(']')
    #print(b, e, s[b+1:e])
    if b < 0:
        return("not labeled")
    return s[b+1:e]



# should Get the table name from the Lambda Environment Variable
table_name = 'dbgtesttable'
table = boto3.resource('dynamodb').Table(table_name)

def grabTopic(s):
    print("s=",s)
    b = s.find('[')
    e = s.find(']')
    #print(b, e, s[b+1:e])
    if b < 0:
        return("not labeled")
    return s[b+1:e]


    
def lambda_handler(event, context):
    tstart = time.time()
    # TODO implement
    print(event)
    r = eval(event)
    print("r=", r)
    i = 0
    for z in r:
        # Get the timestamp.
        #print("body=", z)
        try:
            if z[1] == 'bio':
                #print('got bio ', z[1])
                #print('subj=',z[1], ' item = ', z[0])
                t1  = time.time()
                outlist = mod.runit([z[0]])
                t2 = time.time()
                #print('call time =', t2-t1, "outlist =", outlist)

                #if sending to remote service:
                #res = sendrest('bio', [z[0]])
               
                ts = time.time()
                id = str(ts)[5:]
                s = z[0]
                top = grabTopic(s)
                x = outlist
                #Write to DynamoDB.
                item={'id':id, 'category':top, 'cpredicted':x[0][1], 'cmain predict':z[1], 'document': s}
                table.put_item(Item=item)
            else:
                print("got other =", z[1])
        except:
            print("bad call to service")
        
        i +=1
    tend = time.time()
    print("starttime=",  tstart, " endtime=", tend, ' duration=', tend-tstart)
    return {
        'statusCode': 200,
        'body': json.dumps(event)
    }

#for testing
#event = '[("genomics is about dna and rna", "bio"), ("darwin invented the theory of evolution", "bio"), ("your doctor will fix your organs", "bio")]'
#lambda_handler(event, '')
