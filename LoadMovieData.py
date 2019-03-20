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

import os, sys, time, decimal
from decimal import *
import boto3
import json
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Movies')


def loadfile(infile):
    jsonobj = json.load(open(infile))
    lc = 1
    for movie in jsonobj:
        lc += 1
        CreateTime = int(time.time())
        ExpireTime = CreateTime + (1* 60* 60)
        response = table.put_item(
           Item={
                'Year': decimal.Decimal(movie['year']),
                'Title': movie['title'],
                'info': json.dumps(movie['info']),
                'CreateTime': CreateTime,
                'ExpireTime': ExpireTime
            }
        )
        if (lc % 10) == 0:
            print ("%d rows inserted" % (lc))

if __name__ == '__main__':
    filename = sys.argv[1]
    if os.path.exists(filename):
        # file exists, continue
        loadfile(filename)
    else:
        print ('Please enter a valid filename')