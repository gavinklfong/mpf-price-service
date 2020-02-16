'use strict';

const moment = require('moment');
const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const docClient = new aws.DynamoDB.DocumentClient();

const MPF_CATALOG_TABLE = "MPFCatalog";

module.exports.trusteeList = async event => {

  let queryData = null;

  try {
     queryData = await getTrusteeList();

  } catch (e) {
      console.error("Unable to retrieve trustee information. Error:", JSON.stringify(e));

      return {
        statusCode: 500,
        body: JSON.stringify(e)
      };
  }

  return prepareResponse(200, queryData);

};


module.exports.trustee = async event => {

  let trustee = event.pathParameters.trustee;
  trustee = decodeURIComponent(trustee);
  console.log("received parameter: " + trustee);

  let queryData = null;

  try {
     queryData = await getMPFCatalog(trustee);

  } catch (e) {
      console.error("Unable to retrieve trustee information. Error:", JSON.stringify(e));

      return {
        statusCode: 500,
        body: JSON.stringify(e)
      };
  }

  return prepareResponse(200, queryData);

};


module.exports.scheme = async event => {
  
  let trustee = event.pathParameters.trustee;
  trustee = decodeURIComponent(trustee);

  let scheme = event.pathParameters.scheme;
  scheme = decodeURIComponent(scheme);


  let queryData = null;

  try {
     queryData = await getMPFCatalog(trustee, scheme);

  } catch (e) {
      console.error("Unable to retrieve trustee information. Error:", JSON.stringify(e));

      return {
        statusCode: 500,
        body: JSON.stringify(e)
      };
  }

  return prepareResponse(200, queryData);
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
  
    let timePeriod = "D";
    let dynamoTable = "MPFPriceDaily"
    try {  
      timePeriod = event.queryStringParameters.timePeriod;
      if (timePeriod) {
        console.info("input timePeriod = " + timePeriod);
      } 
    } catch (e) {
      console.info("timePeriod parameter not defined, use default value");
    }  
  
    switch (timePeriod) {
      case "D":
        dynamoTable = "MPFPriceDaily";
        break;
      case "W":
        dynamoTable = "MPFPriceWeekly";
        break;
      case "M":
        dynamoTable = "MPFPriceMonthly";
        break;
    }
  
  
  // Run query on DynamoDB
   let queryData = "'";
   let transformedData = "";
  
    console.log("## retrieve MPF dialy price "  + trusteeSchemeFundId 
    + ", startOfDatePeriod = " + startOfDateMoment.format() 
    + ", endOfDatePeriod = " + endOfDateMoment.format()
    + ", dynamoTable = " + dynamoTable);
  
    let params = {
        TableName: dynamoTable,
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

      transformedData = queryData.Items.map(item => {
        let dateMoment = moment(item.priceDate);
        
        return {
          trusteeSchemeFundId: item.trusteeSchemeFundId,
          trustee: item.trustee,
          scheme: item.scheme,
          fundName: item.fundName,
          priceDate: dateMoment.format("YYYYMMDD"),
          price: item.price
        };
      });

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
  
    return prepareResponse(200, transformedData);

};

function prepareResponse(statusCode, body) {

  return {
    statusCode: statusCode,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true,
      'Content-Type': "application/json"
    },
    body: JSON.stringify(body)

  };
}

async function getTrusteeList() {

  let params = {
    TableName: MPF_CATALOG_TABLE,
    ProjectionExpression: "trustee"
  };

  let queryItemList = [];

  try {
      let queryData = await docClient.scan(params).promise();
      queryItemList = queryData.Items;
      
      let map = new Map();
      let distinctItemList = [];
      queryItemList.forEach(item => {
        if (!map.has(item.trustee)) {
          map.set(item.trustee, true);
          distinctItemList.push(item.trustee);
        }
      });

      queryItemList = distinctItemList;

  } catch (e) {
      console.error("Unable to query. Error:", JSON.stringify(e));
      console.error(e);
  }

  return queryItemList;

}

async function getMPFCatalog(trustee, scheme) {
   
  console.log("parameters: trustee=" + trustee + ", scheme=" + scheme);
  let params = null;

  if (!scheme) {
     params = {
      TableName: MPF_CATALOG_TABLE,
      KeyConditionExpression: "trustee = :id",
      ExpressionAttributeValues: {
          ":id": trustee,
         },
      ProjectionExpression: "trustee, scheme, fund"
    };

  } else {
     params = {
      TableName: MPF_CATALOG_TABLE,
      KeyConditionExpression: "trustee = :id",
      ExpressionAttributeValues: {
          ":id": trustee,
          ":scheme": scheme
         },
      FilterExpression: "scheme = :scheme",
      ProjectionExpression: "trustee, scheme, fund"
    };
  }

  let queryItemList = [];

  try {
      let queryData = await docClient.query(params).promise();
      queryItemList = queryData.Items;

  } catch (e) {
      console.error("Unable to query. Error:", JSON.stringify(e));
      console.error(e);
  }

  return queryItemList;
}