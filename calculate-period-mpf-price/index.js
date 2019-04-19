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

    console.log("Dynamodb stream event received");
    console.log(JSON.stringify((event)));

    let weeklyDateMap = new Map();
    let monthlyDateMap = new Map();


    for (let eventRecord of event.Records) {   
        try {
              if (eventRecord.eventSource == "aws:dynamodb") {
              
                    let trusteeSchemeFundId = eventRecord.dynamodb.Keys.trusteeSchemeFundId.S;
                    let priceDate = +eventRecord.dynamodb.Keys.priceDate.N;

                    // Update weekly date key
                    weeklyDateMap = consolidateWeeklyDate(weeklyDateMap, trusteeSchemeFundId, priceDate);

                    // Update monthly date key
                    monthlyDateMap = consolidateMonthlyDate(monthlyDateMap, trusteeSchemeFundId, priceDate);
              }
        
        } catch (e) {
            console.error(e);
        }
    }

    await calculateWeeklyAveragePriceForFundDateMap(weeklyDateMap),
    await calculateMonthlyAveragePriceForFundDateMap(monthlyDateMap)


    const response = {
        statusCode: 200,
        body: JSON.stringify('Completed'),
    };
    return response;
};

async function calculateWeeklyAveragePriceForFundDateMap(fundDateMap) {

    let averagePriceRecords = [];

    for (let trusteeSchemeFundId of fundDateMap.keys()) {
        console.log("weeklyDateMap - " + trusteeSchemeFundId);
        let datePeriodSet = fundDateMap.get(trusteeSchemeFundId);
        for (let datePeriod of datePeriodSet) {
            console.log("datePeriodSet - " + datePeriod);
            let averagePriceRecord = await calculateWeeklyAveragePrice(trusteeSchemeFundId, datePeriod);
            averagePriceRecords.push(averagePriceRecord);
            console.log("saving average price record to db: " + JSON.stringify(averagePriceRecord));
            await saveFundPrice(averagePriceRecord, "MPFPriceWeekly");

        }
    }

    return averagePriceRecords;
}

async function calculateWeeklyAveragePrice(trusteeSchemeFundId, startOfDate) {

   // calculate average price
   let startOfDateMoment = moment(startOfDate).startOf('week');
   let endOfDateMoment = moment(startOfDateMoment).endOf('week');
    
    console.log("## calculate weekly average price for id = "  + trusteeSchemeFundId + ", startOfDatePeriod = " + startOfDateMoment.format());

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

async function calculateMonthlyAveragePriceForFundDateMap(fundDateMap) {

    let averagePriceRecords = [];

    for (let trusteeSchemeFundId of fundDateMap.keys()) {
        console.log("monthlyDateMap - " + trusteeSchemeFundId);
        let datePeriodSet = fundDateMap.get(trusteeSchemeFundId);
        for (let datePeriod of datePeriodSet) {
            console.log("datePeriodSet - " + datePeriod);
            let averagePriceRecord = await calculateMonthlyAveragePrice(trusteeSchemeFundId, datePeriod);
            averagePriceRecords.push(averagePriceRecord);

            console.log("saving average price record to db: " + JSON.stringify(averagePriceRecord));
            await saveFundPrice(averagePriceRecord, "MPFPriceMonthly");

        }
    }

    return averagePriceRecords;

}

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
        ProjectionExpression: "trusteeSchemeFundId, trustee, scheme, fundName, priceDate, priceDateDisplay, price"
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




async function testQuery() {var params = {
        TableName: "MPFPriceDaily",
        // IndexName: "trustee_date_index",
        KeyConditionExpression: "trusteeSchemeFundId = :id and priceDate between :startDate and :endDate",
        // FilterExpression: "fundName = :fundName",
        ExpressionAttributeValues: {
            ":id": "HSBC-SuperTrust Plus-Asia Pacific Equity Fund",
            ":startDate": 1546358400000,
            ":endDate": 1548358400000,
            // ":fundName": "HSI"
        },
        ProjectionExpression: "trusteeSchemeFundId, trustee, fundName, priceDate, priceDateDisplay, price"
    };

    let mpfPriceRecord = {};
    let sum = 0;
    let count = 0;

    try {
        const queryData = await docClient.query(params).promise();
        console.log("Query succeeded.");
        queryData.Items.forEach(function(item) {
            console.log(JSON.stringify(item));
            sum += item.price;
            count++;
        });

        if (count > 0) averagePrice = sum / count;
        console.log("average price = " + averagePrice);
        // mpfPriceRecord = {"trustee": queryData.Items[0].trustee, "scheme": queryData.Items[0].scheme, "fundName": queryData.Items[0].fundName, "priceDate": startOfDateMoment.valueOf(), "priceDateDisplay": startOfDateMoment.format("YYYY-MM-DD"), "price": averagePrice};

        // console.log("mpfPriceRecord: " + JSON.stringify(mpfPriceRecord));

    } catch (e) {
        console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
    }
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