const moment = require('moment');
const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

// aws.config.update({
//   region: "local",
//   endpoint: "http://localhost:8000"
// });

const dynamodb = new aws.DynamoDB;
const docClient = new aws.DynamoDB.DocumentClient();

const DEFAULT_DAILY_TABLE_NAME = "MPFPriceDaily";
const DEFAULT_MONTHLY_TABLE_NAME = "MPFPriceMonthly";
const DEFAULT_WEEKLY_TABLE_NAME = "MPFPriceWeekly";

exports.handler = async (event) => {

    console.log("SQS event received");
    console.log(JSON.stringify((event)));

    for (let eventRecord of event.Records) {   
        try {
                if (eventRecord.eventSource == "aws:sqs") {
                    const message = JSON.parse(eventRecord.body);
                    console.log("Message: " + JSON.stringify(message)); 
                    let averagePriceRecord = await calculateMonthlyAveragePrice(message.trusteeSchemeFundId, message.priceDate);
                    await saveFundPrice(averagePriceRecord, DEFAULT_MONTHLY_TABLE_NAME);
              } else {
                  throw new Error('Unknown event: ' + eventRecord.eventSource);
              }
        
        } catch (e) {
            console.error(e);
            throw e;
        }
    }

    const response = {
        statusCode: 200,
        body: JSON.stringify('Completed.')
    };
    return response;
};

async function calculateMonthlyAveragePrice(trusteeSchemeFundId, startOfDate) {

   // calculate average price
   let startOfDateMoment = moment(startOfDate).startOf('month');
   let endOfDateMoment = moment(startOfDateMoment).endOf('month');
    
    console.log("## calculate monthly average price for id = "  + trusteeSchemeFundId + ", startOfDatePeriod = " + startOfDateMoment.format());

    let params = {
        TableName: "MPFPriceDaily",
        // IndexName: "trustee_date_index",
        KeyConditionExpression: "trusteeSchemeFundId = :id and priceDate between :startDate and :endDate",
        // FilterExpression: "fundName = :fundName",
        ExpressionAttributeValues: {
            ":id": trusteeSchemeFundId,
            ":startDate": startOfDateMoment.valueOf(),
            ":endDate": endOfDateMoment.valueOf(),
                // ":fundName": "HSI"
            },
        ProjectionExpression: "trusteeSchemeFundId, trustee, scheme, fundName, priceDate, dateDisplay, price"
    };

    let mpfPriceRecord = {};
    let sum = 0;
    let count = 0;
    let averagePrice = 0;

    try {
        const queryData = await docClient.query(params).promise();
        console.log("Query succeeded.");
        queryData.Items.forEach(function(item) {
            // console.log(JSON.stringify(item));
            sum = sum + +item.price;
            count++;
        });

        if (count > 0) averagePrice = sum / count;
        console.log("average price = " + averagePrice + ", count = " + count + ", sum = " + sum);
        mpfPriceRecord = {"trusteeSchemeFundId": trusteeSchemeFundId, "trustee": queryData.Items[0].trustee, "scheme": queryData.Items[0].scheme, "fundName": queryData.Items[0].fundName, "priceDate": startOfDateMoment.valueOf(), "priceDateDisplay": startOfDateMoment.format("YYYY-MM-DD"), "price": averagePrice};

        console.log("mpfPriceRecord: " + JSON.stringify(mpfPriceRecord));

    } catch (e) {
        console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    }

    return mpfPriceRecord;
}

async function saveFundPrice(fundPriceRecord, tableName) {

    if (!tableName) {
        tableName = DEFAULT_MONTHLY_TABLE_NAME;
    }
  
    if (!fundPriceRecord.trusteeSchemeFundId) {
        fundPriceRecord.trusteeSchemeFundId = fundPriceRecord.trustee + "-" + fundPriceRecord.scheme + "-" + fundPriceRecord.fundName;
    }
  
    let params = {
        TableName: tableName,
        Item: fundPriceRecord
    };
    console.log("Calling PutItem");
    console.log(JSON.stringify(params));
  
    let result = new Promise((resolve, reject) =>  {
        
        docClient.put(params, function(err, data) {
            if (err)  { 
                console.error(err); // an error occurred
                reject(err);
            }
            else { 
                console.log("PutItem returned successfully");
                resolve(fundPriceRecord.trusteeSchemeFundId);
            }
        });
    });
  
    return await result;
  
  }