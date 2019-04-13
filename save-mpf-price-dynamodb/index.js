var aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

// aws.config.update({
//   region: "local",
//   endpoint: "http://localhost:8000"
// });

var dynamodb = new aws.DynamoDB;
var docClient = new aws.DynamoDB.DocumentClient();

exports.handler = async (event) => {

    console.log("SQS event received");
    console.log(JSON.stringify((event)));

    for (let eventRecord of event.Records) {   
        try {
              if (eventRecord.eventSource == "aws:sqs") {
              
                    const message = JSON.parse(eventRecord.body);
                    console.log("Message: " + JSON.stringify(message));                    
                    await saveFundPrice(message);
              }
        
        } catch (e) {
            console.error(e);
        }

    }

    const response = {
        statusCode: 200,
        body: JSON.stringify('Completed'),
    };
    return response;
};


async function saveFundPrice(fundPriceRecord) {

  if (!fundPriceRecord.id) {
      fundPriceRecord.id = fundPriceRecord.trustee + "-" + fundPriceRecord.fundName + "-" + fundPriceRecord.priceDate;
  }

  let params = {
      TableName: 'MPFPriceDaily',
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
              resolve(fundPriceRecord.id);
          }
      });
  });

  return await result;

}
