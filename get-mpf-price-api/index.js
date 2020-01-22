'use strict';

const moment = require('moment');
const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const dynamodb = new aws.DynamoDB;
const docClient = new aws.DynamoDB.DocumentClient();

module.exports.trustee = async event => {
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'Go Serverless v1.0! Your function executed successfully!',
        input: event,
      },
      null,
      2
    ),
  };

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};

module.exports.scheme = async event => {
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'Go Serverless v1.0! Your function executed successfully!',
        input: event,
      },
      null,
      2
    ),
  };

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};

module.exports.fund = async event => {
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: 'Go Serverless v1.0! Your function executed successfully!',
        input: event,
      },
      null,
      2
    ),
  };

  // Use this code if you don't use the http event with the LAMBDA-PROXY integration
  // return { message: 'Go Serverless v1.0! Your function executed successfully!', event };
};


module.exports.fundPrice = async event => {

// extract input parameters
  let trustee = event.pathParameters.trustee;
  let scheme = event.pathParameters.scheme;
  let fund = event.pathParameters.fund;

  let trusteeSchemeFundId = trustee + "-" + scheme + "-" + fund;
  trusteeSchemeFundId = decodeURIComponent(trusteeSchemeFundId);

  let startOfDateMoment = moment().subtract(1, 'months');
  let startDate = startOfDateMoment.format("YYYYMMDD");
  try {

    startDate = event.queryStringParameters.startDate;
    console.info("input startDate = " + startDate);
    if (startDate) {
      startOfDateMoment = moment(startDate, "YYYYMMDD");
    } 

  } catch (e) {
    console.info("startDate parameter not defined, use default value");
  }

  let endOfDateMoment = moment(startOfDateMoment).add(1, 'months');
  let endDate = endOfDateMoment.format("YYYYMMDD");
  try {  
    endDate = event.queryStringParameters.endDate;
    if (endDate) {
      console.info("input endDate = " + endDate);
      endOfDateMoment = moment(endDate, "YYYYMMDD");
    } 
  } catch (e) {
    console.info("endDate parameter not defined, use default value");
  }


// Run query on DynamoDB
 let queryData = "'";

  console.log("## retrieve MPF dialy price "  + trusteeSchemeFundId + ", startOfDatePeriod = " + startOfDateMoment.format() + ", endOfDatePeriod = " + endOfDateMoment.format());

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

  try {
    queryData = await docClient.query(params).promise();
    console.log("Query succeeded.");
    // queryData.Items.forEach(function(item) {
    //     // console.log(JSON.stringify(item));
    //     sum = sum + +item.price;
    //     count++;
    // });

    // if (count > 0) averagePrice = sum / count;
    // console.log("average price = " + averagePrice + ", count = " + count + ", sum = " + sum);
    // mpfPriceRecord = {"trusteeSchemeFundId": trusteeSchemeFundId, "trustee": queryData.Items[0].trustee, "scheme": queryData.Items[0].scheme, "fundName": queryData.Items[0].fundName, "priceDate": startOfDateMoment.valueOf(), "priceDateDisplay": startOfDateMoment.format("YYYY-MM-DD"), "price": averagePrice};

    // console.log("mpfPriceRecord: " + JSON.stringify(mpfPriceRecord));

  } catch (e) {
      console.error("Unable to query. Error:", JSON.stringify(e, null, 2));

      return {
        statusCode: 500,
        body: JSON.stringify(e, null, 2)
      };
  }

  return {
    statusCode: 200,
    body: JSON.stringify(queryData)

  };


};