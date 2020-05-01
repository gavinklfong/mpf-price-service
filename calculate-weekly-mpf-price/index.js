const moment = require('moment');
const dataAccess = require('./mpf-data-access');
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
                    let averagePriceRecord = await calculateWeeklyAveragePrice(message.trusteeSchemeFundId, message.priceDate);
                    let priceRecordWithPerformance = await calculateAllPerformance(averagePriceRecord);
                    await saveFundPrice(priceRecordWithPerformance, DEFAULT_WEEKLY_TABLE_NAME);
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

async function calculateWeeklyAveragePrice(trusteeSchemeFundId, startOfDate) {

   // calculate average price
   let startOfDateMoment = moment(startOfDate).startOf('week');
   let endOfDateMoment = moment(startOfDateMoment).endOf('week');
    
    console.log("calculateWeeklyAveragePrice() - calculate weekly average price for id = "  + trusteeSchemeFundId + ", startOfDatePeriod = " + startOfDateMoment.format());

    let mpfPriceRecord = {};
    let sum = 0;
    let count = 0;
    let averagePrice = 0;

    try {
        const retrievedPrices = await dataAccess.retrieveFundPriceByDate("D", trusteeSchemeFundId, startOfDateMoment.valueOf(),  endOfDateMoment.valueOf());        
        retrievedPrices.forEach(function(item) {
            sum = sum + +item.price;
            count++;
        });

        if (count > 0) averagePrice = sum / count;
        console.log("calculateWeeklyAveragePrice() - average price = " + averagePrice + ", count = " + count + ", sum = " + sum);
        mpfPriceRecord = {"trusteeSchemeFundId": trusteeSchemeFundId, "trustee": retrievedPrices[0].trustee, "scheme": retrievedPrices[0].scheme, "fundName": retrievedPrices[0].fundName, "priceDate": startOfDateMoment.valueOf(), "priceDateDisplay": startOfDateMoment.format("YYYY-MM-DD"), "price": averagePrice};

        console.log("mpfPriceRecord: " + JSON.stringify(mpfPriceRecord));

    } catch (e) {
        console.error("Unable to query. Error:", JSON.stringify(e, null, 2));
    }

    return mpfPriceRecord;
}

async function calculateAllPerformance(fundPriceRecord) {

    let newPriceRecord = {...fundPriceRecord};

    // Calculate average price for 1 month
    let month1Performance = await calculatePerformance(newPriceRecord, 1);
    newPriceRecord.month1Growth = month1Performance;

    // Calculate average price for 3 month
    let month3Performance = await calculatePerformance(newPriceRecord, 3);
    newPriceRecord.month3Growth = month3Performance;

    // Calculate avarage price for 6 month
    let month6Performance = await calculatePerformance(newPriceRecord, 6);
    newPriceRecord.month6Growth = month6Performance;

    // Calculate average price for 12 month
    let month12Performance = await calculatePerformance(newPriceRecord, 12);
    newPriceRecord.month12Growth = month12Performance;

    return newPriceRecord;

}

async function calculatePerformance(fundPriceRecord, noOfMonth) {

    let endOfDateMoment = moment(fundPriceRecord.priceDate).startOf('week');
    let startOfDateMoment = moment(endOfDateMoment).subtract(noOfMonth, 'months').startOf('week');
    let averagePrice = await calculateAveragePriceForWeeklyPeriod(fundPriceRecord.trusteeSchemeFundId, startOfDateMoment.valueOf(), endOfDateMoment.valueOf(), fundPriceRecord);

    console.log(averagePrice);
    let performance = (fundPriceRecord.price - averagePrice) / averagePrice;
    console.log("calculatePerformance() - performance = " + performance + ", fundPriceRecord.price = " + fundPriceRecord.price  + ", averagePrice = " + averagePrice );

    return performance;
}

async function calculateAveragePriceForWeeklyPeriod(trusteeSchemeFundId, startDate, endDate, fundPriceRecordToAdd) {

    console.log("calculateAveragePriceForWeeklyPeriod() - trusteeSchemeFundId = " + trusteeSchemeFundId + ", startDate = " + moment(startDate).format('YYYY-MM-DD') + ", endDate = " + moment(endDate).format('YYYY-MM-DD'));
    
 
    let mpfPriceRecord = {};
    let sum = 0;
    let count = 0;
    let averagePrice = 0;


    try {

        const retrievedPrices = await dataAccess.retrieveFundPriceByDate("W", trusteeSchemeFundId, startDate, endDate);
        console.log("calculateAveragePriceForWeeklyPeriod() - Query succeeded.");
        retrievedPrices.forEach(function(item) {
            // console.log(JSON.stringify(item));
            sum = sum + +item.price;
            count++;
        });

        if (fundPriceRecordToAdd) {
            count++;
            sum += fundPriceRecordToAdd.price; 
        }

        if (count > 0) averagePrice = sum / count;
        console.log("calculateAveragePriceForWeeklyPeriod() - average price = " + averagePrice + ", count = " + count + ", sum = " + sum);
        
    } catch (e) {
        console.error("Unable to query. Error:", JSON.stringify(e, null, 2));
    }

    return averagePrice;
}
