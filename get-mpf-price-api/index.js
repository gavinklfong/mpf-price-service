const moment = require('moment');
const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const dynamodb = new aws.DynamoDB;
const docClient = new aws.DynamoDB.DocumentClient();


module.exports.handler = async (event, context, callback) => {

  let trusteeSchemeFundId = "HSBC-SuperTrust Plus-North American Equity Fund";
  let endOfDateMoment = moment();
  let startOfDateMoment = moment().subtract(1, 'months');
  let queryData = "'";

  console.log("## retrieve MPF dialy price "  + trusteeSchemeFundId + ", startOfDatePeriod = " + startOfDateMoment.format());

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
      console.error("Unable to query. Error:", JSON.stringify(err, null, 2));
  }

  const response = {
    statusCode: 200,
    body: JSON.stringify(queryData)
    
  //   JSON.stringify({
  //     message: 'Go Serverless v1.0! Your function executed successfully!',
  //     input: event,
  //   }),
  };

  callback(null, response);

};