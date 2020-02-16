const util = require('util');
const csv = require('csv');
const fs = require('fs');
const moment = require('moment');
const axios = require('axios');

const fundConfig = require('./config.json');

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const docClient = new aws.DynamoDB.DocumentClient();

const SqsWriteStream = require('./sqs-write-stream');
const DEFAULT_SQS_QUEUE_NAME = 'mpf-price-service-mpf-price-queue-dev';
const MPF_TRUSTEE = "HSBC";
const MPF_SCHEME = "SuperTrust Plus";

exports.handler = async (event) => {

    if (!event || !(event.Records)) {

      console.log("Triggered by scheduler")

      let startDate = moment().subtract(3, 'weeks').startOf('day');
      let endDate = moment().subtract(1, 'weeks').startOf('day');
  
      try {
        await retrieveMPFPrice(startDate.format('YYYY-MM-DD'), endDate.format('YYYY-MM-DD'));
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

  let funds = await getMPFCatalog(MPF_TRUSTEE);

  for (let i = 0; i < funds.length; i++) {
    console.log("retrieving fund price from HSBC - fund code: " + JSON.stringify(funds[i]));
  
    let url = util.format(fundConfig.url, funds[i].fundCode, startDate, endDate);
    
    let response = await axios.get(url);
     await parseCSVToSqs(response.data);
    
  }

}

function initSqsWriteStream () {

  let queueName = process.env.SQS_QUEUE_NAME;

  if (!queueName || typeof queueName !== 'string') {
    console.log("QueueName in environment variable is empty, use default value"); 

    queueName = DEFAULT_SQS_QUEUE_NAME;
  }

  console.log("QueueName: " + JSON.stringify(queueName)); 

  let sqsStream = new SqsWriteStream(
    {name: queueName},
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
          let recordJson = {"trustee": MPF_TRUSTEE, "scheme": MPF_SCHEME, "fundName": fundName, "priceDate": priceDate.startOf("date").valueOf(), "priceDateDisplay": priceDate.format("YYYY-MM-DD"), "price": record[1]};
          console.log("In Transform: data: " + JSON.stringify(recordJson));
          return recordJson;
    }
        
  }))
  .pipe(sqsStream.stream);

  await sqsStream.finishPromise;

}

async function getMPFCatalog(trustee) {
   
  let params = {
      TableName: "MPFCatalog",
      KeyConditionExpression: "trustee = :id",
      ExpressionAttributeValues: {
          ":id": trustee,
         },
      ProjectionExpression: "trustee, scheme, schemeCode, fund, fundCode"
  };

  let queryItemList = [];

  try {
      queryData = await docClient.query(params).promise();
      console.log("Query succeeded.");
      queryItemList = queryData.Items;

  } catch (e) {
      console.error("Unable to query. Error:", JSON.stringify(e));
      console.error(e);
  }

  return queryItemList;
}