const util = require('util');
const fs = require('fs');
const moment = require('moment');
const axios = require('axios');
const streamify = require('stream-array');

const fundConfig = require('./config.json');

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const docClient = new aws.DynamoDB.DocumentClient();

const SqsWriteStream = require('./sqs-write-stream');
const DEFAULT_SQS_QUEUE_NAME = 'mpf-price-service-mpf-price-queue-dev';
const MPF_TRUSTEE = "BCT";

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

  const sqsStream = initSqsWriteStream();

  const funds = await getMPFCatalog(MPF_TRUSTEE);

  let fundPriceList = [];
  const schemeList = [];
  const map = new Map();
  for (const fund of funds) {
    if (!map.has(fund.schemeCode)) {
      map.set(fund.schemeCode, true);
      schemeList.push({name: fund.scheme, code: fund.schemeCode});
    }
  }

  console.log("Scheme list: " + JSON.stringify(schemeList));

  for (let i = 0; i < schemeList.length; i++) {
    let scheme = schemeList[i];

    console.log("retrieving fund price from BCT - scheme: " + JSON.stringify(scheme));

    const startDateMoment = moment(startDate, 'YYYY-MM-DD');
    const endDateMoment = moment(endDate, 'YYYY-MM-DD');
    
    for (let currentDateMoment = startDateMoment; 
          currentDateMoment.isSameOrBefore(endDateMoment); 
          currentDateMoment.add(1, 'days')) {

            console.log("get fund price for scheme=" + scheme.name + ", date=" + startDateMoment.format("YYYY-MM-DD"));
            let fundPriceOfCurrentDate = await retrieveMPFPriceByScheme(scheme.name, scheme.code, currentDateMoment);
            // console.log(fundPriceOfCurrentDate);
            fundPriceList = [...fundPriceList, ...fundPriceOfCurrentDate];
    }

  };

  console.log("Fund price result:");
  console.log(fundPriceList);

  // output to SQS
  streamify(fundPriceList).pipe(sqsStream.stream);

  await sqsStream.finishPromise;


}

async function retrieveMPFPriceByScheme(schemeName, schemeCode, priceDateMoment) {
  
  let fundPriceList = [];

  const REQ_CONFIG = {
    headers: fundConfig.headers
  };

  const reqBody = util.format(fundConfig.body, schemeCode, priceDateMoment.format("YYYY-MM-DD"));
  console.log("POST request body = " + reqBody);

  try {
      const response = await axios({
        method: fundConfig.method,
        url: fundConfig.url,
        data: reqBody,
        REQ_CONFIG
      });

    if (response.status == 200) {

        if ((response.data.data) && (response.data.data.funds)) {
          let respFunds = response.data.data.funds;
          fundPriceList =
              respFunds.map(item => {
                return { 
                  trustee: MPF_TRUSTEE, 
                  scheme: schemeName, 
                  fundName: item.name, 
                  priceDate: priceDateMoment.valueOf(), 
                  priceDateDisplay: priceDateMoment.format("YYYY-MM-DD"), 
                  price: item.price
                }
              });
        }

    } else {
      console.error("Error response is received. status=" + response.status + ", date=" + priceDateMoment.format("YYYY-MM-DD") + ", scheme=" + schemeName);
    }

  } catch (e) {
    console.error("Request error. errror code =" + e.code + ", message = " + e.message);
  }

  // console.log(fundPriceList);

  return fundPriceList;
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