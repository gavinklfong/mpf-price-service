const util = require('util');
const csv = require('csv');
const fs = require('fs');
const moment = require('moment');
const axios = require('axios');

const fundConfig = require('./config.json');

var aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const SqsWriteStream = require('./sqs-write-stream');


var s3 = new aws.S3();


exports.handler = async (event) => {

    console.log("SQS event received");
    console.log(JSON.stringify((event)));

    for (let eventRecord of event.Records) {   
        try {
              if (eventRecord.eventSource == "aws:sqs") {
              
                    const message = JSON.parse(eventRecord.body);
                    console.log("Message: " + JSON.stringify(message));                    
                    await retrieveMPFPrice(message.startDate, message.endDate);
              }
        
        } catch (e) {
            console.error(e);
        }
    } 
};

async function retrieveMPFPrice(startDate, endDate) {

  // var fundConfig = await getObject("mpf-data", "/config.json");

  for (let i = 0; i < fundConfig.funds.length; i++) {
    console.log(fundConfig.funds[i]);
  
    let url = util.format(fundConfig.url, fundConfig.funds[i], startDate, endDate);
  
    console.log(url);
  
    let response = await axios.get(url);
    // console.log(response.data);
     await parseCSV(response.data);
    
  }

}

async function parseCSV(content) {

  let sqs = new SqsWriteStream(
    {url: "https://sqs.us-east-2.amazonaws.com/041740121314/MPFPriceQueue"},
    {batchSize: 10}
  );
  
  sqs.on("msgReceived", (data) => {
        console.log("sqs msgReceived: " + JSON.stringify(data));
  });
  
  sqs.on("msgProcessed", (data) => {
        console.log("sqs msgProcessed: " + JSON.stringify(data));
  });
  
  let sqsEnd = new Promise(function(resolve, reject) {
    sqs.on('finish', () => {
      console.log('All SQS writes are now complete.');
      resolve("Finished");
    });
    sqs.on('error', reject); 
  });


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
          fundName = fundName.replace(/[^\x00-\x7F]/g,"");
          isHeader = false;
          return null;
    } else {
          let priceDate = moment(record[0], "YYYY-MM-DD");
          let recordJson = {"trustee": "HSBC", "fundName": fundName, "priceDate": priceDate.startOf("date").valueOf(), "priceDateDisplay": record[0], "price": record[1]};
          console.log("In Transform: data: " + JSON.stringify(recordJson));
          return recordJson;
    }
        
  }))
  // .pipe(csv.stringify())
  // .pipe(process.stdout);
  .pipe(sqs);

  let sqsEndResult = await sqsEnd;

  console.log("pipeline completed");

  const response = {
    statusCode: 200,
    body: JSON.stringify('Completed'),
  };
  return response;
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