'use strict';

const moment = require('moment');
const mpfDataAccess = require('./mpf-data-access');

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const docClient = new aws.DynamoDB.DocumentClient();

module.exports.trusteeList = async event => {

  let queryData = null;

  try {
     queryData = await mpfDataAccess.getTrusteeList();

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
     queryData = await mpfDataAccess.getMPFCatalog(trustee);

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
     queryData = await mpfDataAccess.getMPFCatalog(trustee, scheme);

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

  if (!event.body ||  event.body === "undefined" ) {
    return {
      statusCode: 404,
      message: "Invalid parameter"
    }
  }

  console.log(event.body);

  let reqBody = JSON.parse(event.body);
  let startDate = reqBody.startDate;
  let endDate = reqBody.endDate;
  let timePeriod = reqBody.timePeriod;
  let funds = reqBody.funds;

  let startOfDateMoment = moment().subtract(1, 'months');
  try {

    console.info("input startDate = " + startDate);
    if (startDate) {
      startOfDateMoment = moment(startDate, "YYYYMMDD");
    } 

  } catch (e) {
    console.info("startDate parameter not defined, use default value");
  }

  let endOfDateMoment = moment(startOfDateMoment).add(1, 'months');
  try {  
    if (endDate) {
      console.info("input endDate = " + endDate);
      endOfDateMoment = moment(endDate, "YYYYMMDD");
    } 
  } catch (e) {
    console.info("endDate parameter not defined, use default value");
  }

  try {  
    if (timePeriod) {
      console.info("input timePeriod = " + timePeriod);
    } else {
      timePeriod = "D";
    }
  } catch (e) {
    console.info("timePeriod parameter not defined, use default value");
  }  

  if (funds && funds.length > 0) {
    let result$ = mpfDataAccess.retrieveFundPrices(funds, startDate, endDate, timePeriod);

    // Convert observable to be promise. capture all the data and return
    let resultPromise = new Promise((resolve, reject) => {
        let output = [];
        result$.subscribe({
          next(x) { output = output.concat(x) },
          error(err) { console.error('something wrong occurred: ' + err); reject(err); },
          complete() { resolve(output); }
        })
    });

    // toPromise() does not work, it resolves once first data is received without waiting for observable to complete
    // let resultPromise = result$.toPromise();

    let result = prepareResponse(200, await resultPromise);
    return result;

  } else {
    return {
      statusCode: 404,
      message: "Invalid parameter"
    }
  }
}


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

