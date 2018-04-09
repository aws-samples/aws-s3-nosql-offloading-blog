

#########################################################################################
#  Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this
#  software and associated documentation files (the "Software"), to deal in the Software
#  without restriction, including without limitation the rights to use, copy, modify,
#  merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
#  permit persons to whom the Software is furnished to do so.
# 
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
#  INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
#  PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
#  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
#  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#########################################################################################

import os, json, base64, boto3

firehose = boto3.client('firehose')

print('Loading function')

def recToFirehose(streamRecord):
    ddbRecord = streamRecord['NewImage']
    toFirehose = {}
    for c in ddbRecord:
        toFirehose[c] = next(iter(ddbRecord[c].values()))
    jddbRecord = json.loads(ddbRecord['info']['S'])
    # Transform the record a bit
    try:
        rating = jddbRecord['rating']
    except:
        rating = 0
    try:
        actors = jddbRecord['actors']
    except:
        actors = [' ',' ']
    actor1 = actors[0]
    try:
        actor2 = actor[1]
    except:
        actor2 = ' '
    try:
        genres = jddbRecord['genres']
    except:
        genres = ['','']
    genre1 = genres[0]
    try:
        genre2 = genres[1]
    except:
        genre2 = ' '
    
    try:
        directors = jddbRecord['directors']
    except:
        directors = [' ',' ']
    director1 = directors[0]
    try:
        director2 = directors[1]
    except:
        director2 = ' '
    
    toFirehose["actor1"] = actor1
    toFirehose["actor2"] = actor2
    toFirehose["director1"] = director1
    toFirehose["director2"] = director2
    toFirehose["genre1"] = genre1
    toFirehose["genre2"] = genre2
    toFirehose["rating"] = rating
    jtoFirehose = json.dumps(toFirehose)
    response = firehose.put_record(
    DeliveryStreamName=os.environ['DeliveryStreamName'],
    Record= {
                'Data': jtoFirehose + '\n'
            }
        )
    print(response)

def lambda_handler(event, context):
    for record in event['Records']:
        if (record['eventName']) != 'REMOVE':
            recToFirehose(record['dynamodb'])
    return 'Successfully processed {} records.'.format(len(event['Records']))
