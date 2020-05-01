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

const DEFAULT_TABLE_NAME = "MPFPriceDaily";

exports.handler = async (event) => {

    console.log("event received");
    console.log(JSON.stringify((event)));

    for (let eventRecord of event.Records) {   
        try {              
                const message = JSON.parse(eventRecord.Sns.Message);
                console.log("Message: " + JSON.stringify(message));                    
                
                // calculate technical analysis
                const fundPrice = await calculateAllPerformance(message.trusteeSchemeFundId, moment().valueOf());

                console.log(fundPrice);

                // save to MPFPriceDaily
                await dataAccess.saveFundPerformance(fundPrice);

        } catch (e) {
            console.error(e);
            throw e;
        }

    }

    const response = {
        statusCode: 200,
        body: JSON.stringify('Completed'),
    };
    return response;
};

async function calculateAllPerformance(trusteeSchemeFundId, priceDate) {

    const idArray = trusteeSchemeFundId.split("-");    
    let newPriceRecord = {trusteeSchemeFundId: trusteeSchemeFundId, trustee: idArray[0], scheme: idArray[1], fund: idArray[2]};

    // Calculate average price for 1 month
    let month1Performance = await calculatePerformance(trusteeSchemeFundId, priceDate, 1);
    newPriceRecord.month1Growth = month1Performance;

    // Calculate average price for 3 month
    let month3Performance = await calculatePerformance(trusteeSchemeFundId, priceDate, 3);
    newPriceRecord.month3Growth = month3Performance;

    // Calculate avarage price for 6 month
    let month6Performance = await calculatePerformance(trusteeSchemeFundId, priceDate, 6);
    newPriceRecord.month6Growth = month6Performance;

    // Calculate average price for 12 month
    let month12Performance = await calculatePerformance(trusteeSchemeFundId, priceDate, 12);
    newPriceRecord.month12Growth = month12Performance;

    return newPriceRecord;

}

async function calculatePerformance(trusteeSchemeFundId, priceDate, noOfMonth) {

    let endOfDateMoment = moment(priceDate).startOf('week');
    let startOfDateMoment = moment(endOfDateMoment).subtract(noOfMonth, 'months').startOf('week');
    let averagePrice = await calculateAveragePrice(trusteeSchemeFundId, startOfDateMoment.valueOf(), endOfDateMoment.valueOf());
    let latestPrice = await retrieveLatestPrice(trusteeSchemeFundId);

    let performance = (latestPrice - averagePrice) / averagePrice;
    console.log("calculatePerformance() - performance = " + performance + ", fundPriceRecord.price = " + latestPrice  + ", averagePrice = " + averagePrice );

    return performance;
}

async function calculateAveragePrice(trusteeSchemeFundId, startDate, endDate) {

    console.log("calculateAveragePrice() - trusteeSchemeFundId = " + trusteeSchemeFundId + ", startDate = " + moment(startDate).format('YYYY-MM-DD') + ", endDate = " + moment(endDate).format('YYYY-MM-DD'));
     
    let sum = 0;
    let count = 0;
    let averagePrice = 0;

    try {

        const retrievedPrices = await dataAccess.retrieveFundPriceByDate("D", trusteeSchemeFundId, startDate, endDate);
        console.log("calculateAveragePrice() - Query succeeded.");
        retrievedPrices.forEach(function(item) {
            // console.log(JSON.stringify(item));
            sum = sum + +item.price;
            count++;
        });

        if (count > 0) averagePrice = sum / count;
        console.log("calculateAveragePrice() - average price = " + averagePrice + ", count = " + count + ", sum = " + sum);
        
    } catch (e) {
        console.error("calculateAveragePrice() - Unable to query. Error:", JSON.stringify(e, null, 2));
    }

    return averagePrice;
}


const retrieveLatestPrice = async (trusteeSchemeFundId) => {
    const endDateMoment = moment();
    const startDateMoment = moment();
    startDateMoment.subtract(2, 'week');
    fundPrices = await dataAccess.retrieveFundPriceById(trusteeSchemeFundId, startDateMoment.format("YYYYMMDD"), endDateMoment.format("YYYYMMDD"));

    fundPrices.sort((a, b) => {
        if (b.priceDate > a.priceDate) {
            return 1;
        } else if (b.priceDate < a.priceDate) {
            return -1;
        } else {
            return 0;
        }
    });

    return fundPrices[0].price;
}


