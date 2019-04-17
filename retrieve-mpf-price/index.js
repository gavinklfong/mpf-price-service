const util = require('util');
const csv = require('csv');
const fs = require('fs');
const moment = require('moment');
const axios = require('axios');

const fundConfig = require('./config.json');

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const SqsWriteStream = require('./sqs-write-stream');

const s3 = new aws.S3();

const DEFAULT_SQS_URL = 'https://sqs.us-east-2.amazonaws.com/041740121314/MPFPriceQueue';

exports.handler = async (event) => {

    if (!event || !(event.Records)) {

      console.log("Triggered by scheduler")

      let startDate = moment().subtract(3, 'weeks').startOf('day');
      let endDate = moment().subtract(1, 'weeks').startOf('day');
  
      try {
        await retrieveMPFPrice(startDate, endDate);
      } catch (e) {
        console.error(e);
        throw e;
      }
    } else {

      console.log("Triggered by sqs");

      for (let eventRecord of event.Records) {   
          try {
                if (eventRecord.eventSource == "aws:sqs") {
                
                      const message = JSON.parse(eventRecord.body);
                      console.log("Message: " + JSON.stringify(message));                    
                      await retrieveMPFPrice(message.startDate, message.endDate);
                }
          
          } catch (e) {
              console.error(e);
              throw e;
          }
      } 
    }
};

async function retrieveMPFPrice(startDate, endDate) {

  // var fundConfig = await getObject("mpf-data", "/config.json");

  for (let i = 0; i < fundConfig.funds.length; i++) {
    console.log(fundConfig.funds[i]);
  
    let url = util.format(fundConfig.url, fundConfig.funds[i], startDate, endDate);
  
    console.log('retrieve MPF price from: ' + url);
  
    let response = await axios.get(url);
     await parseCSVToSqs(response.data);
    
  }

}

function initSqsWriteStream () {

  let sqsUrl = process.env.SQS_URL;

  if (!sqsUrl) {
    sqsUrl = DEFAULT_SQS_URL;
  }

  let sqsStream = new SqsWriteStream(
    {url: sqsUrl},
    {batchSize: 10}
  );
  
  sqsStream.on("Sqs msgReceived", (data) => {
        console.log("sqs msgReceived: " + JSON.stringify(data));
  });
  
  sqsStream.on("Sqs msgProcessed", (data) => {
        console.log("sqs msgProcessed: " + JSON.stringify(data));
  });
  
  let sqsEnd = new Promise(function(resolve, reject) {
    sqsStream.on('finish', () => {
      console.log('All SQS writes are now complete.');
      resolve("Finished");
    });
    sqsStream.on('error', reject); 
  });

  let sqsWriteStream = {stream: sqsStream, finishPromise: sqsEnd};

  return sqsWriteStream;
}

async function parseCSVToSqs(content) {

  let sqsStream = initSqsWriteStream();

  let fundName = '';
  let isHeader = true;

  // await pipeline(
    csv.parse(content, {
    delimiter: ',',
    trim: true,
    skip_empty_lines: true,
  })
  .pipe(
  csv.transform (function(record) {

    if (isHeader) {
          fundName = record[0];
          fundName = fundName.replace(/[^\x00-\x7F]/g,"");  // remove non-displayable character
          isHeader = false;
          return null;
    } else {
          let priceDate = moment(record[0], "YYYY-MM-DD");
          let recordJson = {"trustee": "HSBC", "scheme": "SuperTrust Plus", "fundName": fundName, "priceDate": priceDate.startOf("date").valueOf(), "priceDateDisplay": priceDate.format("YYYY-MM-DD"), "price": record[1]};
          console.log("In Transform: data: " + JSON.stringify(recordJson));
          return recordJson;
    }
        
  }))
  .pipe(sqsStream.stream);

  await sqsStream.finishPromise;

}



async function getObject (bucket, objectKey) {
  try {
    const params = {
      Bucket: bucket,
      Key: objectKey 
    }

    const data = await s3.getObject(params).promise();

    return data.Body.toString('utf-8');
  } catch (e) {
    throw new Error(`Could not retrieve file from S3: ${e.message}`)
  }
}