var aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const lambda = new AWS.Lambda();

exports.handler = async (event) => {

    console.log("Scheduler triggered");

    const response = {
        statusCode: 200,
        body: JSON.stringify('Hello'),
    };
    return response;
};

