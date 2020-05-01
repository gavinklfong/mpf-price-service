'use strict';

const moment = require('moment');
const { from  } = require('rxjs');
const { map, flatMap } = require('rxjs/operators');
const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const docClient = new aws.DynamoDB.DocumentClient();

const MPF_CATALOG_TABLE = "MPFCatalog";



const retrieveFundPrices = (funds, startDate, endDate, timePeriod = "M") => {

  let funds$ = from(funds);
  let stream$ = funds$.pipe(
      // tap(fund => console.log(fund)),
      map(fund => { 
          return {trustee: fund.trustee, scheme: fund.scheme, fund: fund.fund, startDate: startDate, endDate: endDate}
      }),
      // tap(fund => console.log(fund)),
      map(query => { return from(retrieveFundPrice(query.trustee, query.scheme, query.fund, query.startDate, query.endDate, timePeriod))}),
      // tap(fund => console.log(fund)),
      flatMap(item => item),
      // tap(item => console.log(item)),
      map(item => formatFundPrices(item))
  );

  return stream$;

};


const formatFundPrices = (items) => {
  let fundMap = new Map();
  let priceMap = new Map();

  for (let i = 0; i < items.length; i++) {
      let item = items[i];

      let prices = [];
      if (priceMap.has(item.trusteeSchemeFundId)) {
          prices = priceMap.get(item.trusteeSchemeFundId);
      } 
      prices.push({priceDate: item.priceDate, price: item.price});
      priceMap.set(item.trusteeSchemeFundId, prices);
      fundMap.set(item.trusteeSchemeFundId, {trustee: item.trustee, scheme: item.scheme, fund: item.fund});
  }

  let output = [];
  for (const key of fundMap.keys()) {
      console.log(key);

      let prices = priceMap.get(key)
      prices.sort((e1, e2) => {
          let e1Date = +e1.priceDate;
          let e2Date = +e2.priceDate;
          if (e1Date < e2Date) {
              return -1
          } else if (e1Date > e2Date) {
              return 1
          }
          return 0;
      })

      let entry = fundMap.get(key);
      let item = {
          trustee: entry.trustee,
          scheme: entry.scheme,
          fund: entry.fund,
          prices: priceMap.get(key)
      }
      output.push(item);
  }

  return output;
};

const retrieveFundPrice = async (trustee, scheme, fund, startDate, endDate, timePeriod = "D") => {

  let trusteeSchemeFundId = trustee + "-" + scheme + "-" + fund;
  trusteeSchemeFundId = decodeURIComponent(trusteeSchemeFundId);
  return await retrieveFundPriceById(trusteeSchemeFundId, startDate, endDate, timePeriod);

}

const retrieveFundPriceById = async (trusteeSchemeFundId, startDate, endDate, timePeriod = "D") => {

  let startOfDateMoment = moment();
  try {
    console.info("input startDate = " + startDate);
    if (startDate) {
      startOfDateMoment = moment(startDate, "YYYYMMDD");
    } else {
        startOfDateMoment = moment().subtract(1, 'months');      
    }

  } catch (e) {
    console.info("startDate parameter not defined, use default value");
  }

  let endOfDateMoment = moment();
  try {  
    if (endDate) {
      console.info("input endDate = " + endDate);
      endOfDateMoment = moment(endDate, "YYYYMMDD");
    }  else {
        endOfDateMoment = moment(startOfDateMoment).add(1, 'months');
    }
  } catch (e) {
    console.info("endDate parameter not defined, use default value");
  }

  let dynamoTable = "MPFPriceDaily"
  try {  
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

    let transformedData = queryData.Items.map(item => {
      let dateMoment = moment(item.priceDate);
      
      return {
        trusteeSchemeFundId: item.trusteeSchemeFundId,
        trustee: item.trustee,
        scheme: item.scheme,
        fund: item.fundName,
        priceDate: dateMoment.format("YYYYMMDD"),
        price: item.price
      };
    });

    return transformedData;
    

  } catch (e) {
      console.error("Unable to query. Error:", JSON.stringify(e));
      console.error(e);
      throw e;
  }

};


const getMPFCatalog = async (trustee, scheme) => {
   
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

const getTrusteeList = async () => {

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

async function retrieveFundPriceByDate(period, trusteeSchemeFundId, startDate, endDate) {

  let dbTable = "MPFPriceWeekly";
  switch (period) {
      case "D":
          dbTable = "MPFPriceDaily";
          break;
      case "W":
          dbTable = "MPFPriceWeekly";
          break;
      case "Y":
          dbTable = "MPFPriceYearly";
          break;
      default:
          dbTable = "MPFPriceWeekly";
  }


  const res = [];
  
  const params = {
      TableName: dbTable,
      KeyConditionExpression: "trusteeSchemeFundId = :id and priceDate between :startDate and :endDate",
      ExpressionAttributeValues: {
          ":id": trusteeSchemeFundId,
          ":startDate": startDate,
          ":endDate": endDate,
          },
      ProjectionExpression: "trusteeSchemeFundId, trustee, scheme, fundName, priceDate, dateDisplay, price"
  };

  const queryData = await docClient.query(params).promise();
  queryData.Items.forEach(function(item) {
      res.push({trusteeSchemeFundId: item.trusteeSchemeFundId, trustee: item.trustee, scheme: item.scheme, fundName: item.fundName,
      priceDate: item.priceDate, dateDisplay: item.dateDisplay, price: item.price});
  });

  return res;
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
  console.log("saveFundPrice() - Calling PutItem");
  console.log(JSON.stringify(params));

  let result = new Promise((resolve, reject) =>  {
      
      docClient.put(params, function(err, data) {
          if (err)  { 
              console.error(err); // an error occurred
              reject(err);
          }
          else { 
              console.log("saveFundPrice() - PutItem returned successfully");
              resolve(fundPriceRecord.trusteeSchemeFundId);
          }
      });
  });

  return await result;

}


async function saveFundPerformance(fundPriceRecord) {

  if (!fundPriceRecord.trusteeSchemeFundId) {
      fundPriceRecord.trusteeSchemeFundId = fundPriceRecord.trustee + "-" + fundPriceRecord.scheme + "-" + fundPriceRecord.fundName;
  }

  let params = {
      TableName: "MPFFundPerformance",
      Item: fundPriceRecord
  };
  console.log("saveFundPerformance() - Calling PutItem");
  console.log(JSON.stringify(params));

  let result = new Promise((resolve, reject) =>  {
      
      docClient.put(params, function(err, data) {
          if (err)  { 
              console.error(err); // an error occurred
              reject(err);
          }
          else { 
              console.log("saveFundPerformance() - PutItem returned successfully");
              resolve(fundPriceRecord.trusteeSchemeFundId);
          }
      });
  });

  return await result;

}

module.exports = {
  retrieveFundPrices,
  getMPFCatalog,
  getTrusteeList,
  retrieveFundPriceByDate,
  retrieveFundPriceById,
  saveFundPerformance,
  saveFundPrice
}