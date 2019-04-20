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

                // save to MPFPriceDaily

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


async function saveFundPrice(fundPriceRecord) {

  let tableName = process.env.DEFAULT_TABLE_NAME;
  if (!tableName) {
      tableName = DEFAULT_TABLE_NAME;
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
