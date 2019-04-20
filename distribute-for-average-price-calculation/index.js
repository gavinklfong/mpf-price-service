const moment = require('moment');
const aws = require('aws-sdk');
const uuidv4 = require('uuid/v4');

aws.config.update({region: 'us-east-2'});

// aws.config.update({
//   region: "local",
//   endpoint: "http://localhost:8000"
// });

// const dynamodb = new aws.DynamoDB;
// const docClient = new aws.DynamoDB.DocumentClient();

const sqs = new aws.SQS();

const sns = new aws.SNS();

const DEFAULT_DAILY_TABLE_NAME = "MPFPriceDaily";
const DEFAULT_MONTHLY_TABLE_NAME = "MPFPriceMonthly";
const DEFAULT_WEEKLY_TABLE_NAME = "MPFPriceWeekly";

const DEFAULT_MONTHLY_PRICE_QUEUE = "mpf-price-service-monthly-mpf-price-queue-dev";
const DEFAULT_WEEKLY_PRICE_QUEUE = "mpf-price-service-weekly-mpf-price-queue-dev";

exports.handler = async (event) => {

    if (!process.env.MONTHLY_PRICE_QUEUE || !process.env.WEEKLY_PRICE_QUEUE) {
        throw new Error('MONTHLY_PRICE_QUEUE or WEEKLY_PRICE_QUEUE is missing in env variable');
    }

    console.log("Dynamodb stream event received");
    console.log(JSON.stringify((event)));

    let weeklyDateMap = new Map();
    let monthlyDateMap = new Map();


    for (let eventRecord of event.Records) {   
        try {
              if (eventRecord.eventSource == "aws:dynamodb") {
              
                    // Skip the record if price is unchanged
                    if (eventRecord.eventName == "MODIFY") {
                        try {
                            let oldPrice = +eventRecord.dynamodb.OldImage.price.S;
                            let newPrice = +eventRecord.dynamodb.NewImage.price.S;
                            if (oldPrice == newPrice) {
                                continue;
                            }
                        } catch (e) {
                            console.log('price attribute does not exist in "MODIFY" event');
                        }
                    }

                    let trusteeSchemeFundId = eventRecord.dynamodb.Keys.trusteeSchemeFundId.S;
                    let priceDate = +eventRecord.dynamodb.Keys.priceDate.N;

                    // Send daily price record key to topic
                    await sendToDailyPriceUpdateTopic({'trusteeSchemeFundId': trusteeSchemeFundId, 'priceDate': priceDate});

                    // Update weekly date key
                    weeklyDateMap = consolidateWeeklyDate(weeklyDateMap, trusteeSchemeFundId, priceDate);

                    // Update monthly date key
                    monthlyDateMap = consolidateMonthlyDate(monthlyDateMap, trusteeSchemeFundId, priceDate);
              }
        
        } catch (e) {
            console.error(e);
            throw e;
        }
    }

    // Send to queue for weekly and monthly price update
    let weeklyRecords = await sendToWeeklyPriceRecordQueue(weeklyDateMap);
    let monthlyRecords = await sendToMonthlyPriceRecordQueue(monthlyDateMap);

    const response = {
        statusCode: 200,
        body: JSON.stringify('Completed. Weekly record processed = ' + weeklyRecords.length + ', Monthly record processed = ' + monthlyRecords.length)
    };
    return response;
};

async function sendToDailyPriceUpdateTopic(priceRecordKey) {

  let topicArn = process.env.DAILY_PRICE_TOPIC;

  let params = {
    Message: JSON.stringify(priceRecordKey), 
    TopicArn: topicArn
  };

  try {
    await sns.publish(params).promise();
  } catch (e) {
    console.error(e);
    throw e;
  }
}


async function sendQueueMessage(record, queueUrl) {
    let params = {
        // DelaySeconds: 10,
        MessageBody: JSON.stringify(record),
        QueueUrl: queueUrl
      };

    await sqs.sendMessage(params).promise();
}

async function sendToWeeklyPriceRecordQueue(fundDateMap) {
    let priceRecords = [];

    for (let trusteeSchemeFundId of fundDateMap.keys()) {
        console.log("weeklyDateMap - " + trusteeSchemeFundId);
        let datePeriodSet = fundDateMap.get(trusteeSchemeFundId);
        for (let datePeriod of datePeriodSet) {
            console.log("datePeriodSet - " + datePeriod);
            let priceRecord = {'trusteeSchemeFundId': trusteeSchemeFundId, 'priceDate': datePeriod};
            priceRecords.push(priceRecord);
            sendQueueMessage(priceRecord, process.env.WEEKLY_PRICE_QUEUE);
        }
    }

    return priceRecords;
}

async function sendToMonthlyPriceRecordQueue(fundDateMap) {

    let priceRecords = [];

    for (let trusteeSchemeFundId of fundDateMap.keys()) {
        console.log("monthlyDateMap - " + trusteeSchemeFundId);
        let datePeriodSet = fundDateMap.get(trusteeSchemeFundId);
        for (let datePeriod of datePeriodSet) {
            console.log("datePeriodSet - " + datePeriod);
            let priceRecord = {'trusteeSchemeFundId': trusteeSchemeFundId, 'priceDate': datePeriod};
            priceRecords.push(priceRecord);
            sendQueueMessage(priceRecord, process.env.MONTHLY_PRICE_QUEUE);
        }
    }

    return priceRecords;
}


function consolidateWeeklyDate(fundDateMap, trusteeSchemeFundId, priceDate) {
    let weeklyDateSet = fundDateMap.get(trusteeSchemeFundId);
    if (!weeklyDateSet) {
        weeklyDateSet = new Set();
    }
    let weekStartDate = moment(priceDate);
    weeklyDateSet.add(weekStartDate.startOf('week').valueOf());
    fundDateMap.set(trusteeSchemeFundId, weeklyDateSet);

    return fundDateMap;
}


function consolidateMonthlyDate(fundDateMap, trusteeSchemeFundId, priceDate) {
    let monthlyDateSet = fundDateMap.get(trusteeSchemeFundId);
    if (!monthlyDateSet) {
        monthlyDateSet = new Set();
    }
    let monthStartDate = moment(priceDate);
    monthlyDateSet.add(monthStartDate.startOf('month').valueOf());
    fundDateMap.set(trusteeSchemeFundId, monthlyDateSet);

    return fundDateMap;
}

function initSqsWriteStream (queueName) {

    if (!queueName) {
        console.log("QueueName in environment variable is empty, use default value"); 

        queueName = DEFAULT_SQS_QUEUE_NAME;
    }

    console.log("QueueName: " + queueName); 

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