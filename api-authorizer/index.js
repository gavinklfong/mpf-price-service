/*
*   The idea of custom authorizer implementation is from the link below
*   https://greggborodaty.com/custom-aka-lambda-authorizer-for-verifying-client-authorization-tokens-generated-by-firebase/
*/

const aws = require('aws-sdk');
aws.config.update({region: 'us-east-2'});

const firebaseAdmin = require('firebase-admin');

const firebaseSrvAcct = require('./firebase-admin-secret.json');

const initializeFirebaseAdminSdk = function () {
  // Check if Firebase Admin SDK is already initialized, if not, then do it
  if (firebaseAdmin.apps.length == 0) {
    firebaseAdmin.initializeApp({
      credential: firebaseAdmin.credential.cert(firebaseSrvAcct),
      databaseURL: process.env.FIREBASE_DATABASE_URL
    });
  }
}

// Helper funtion for generating the response API Gateway requires to handle the token verification
const generateIamPolicy = (effect, resource, data) => {
    const authResponse = {};
  
    // Define a user object that passes user data and other user informationd decoded from the Firebase token to the Lambda API handler
    const user = {};
  
    // Populate the API Gateway user principalId with the Firebase user id, or 'unavailable' if not returned from Firebase
    data ? authResponse.principalId = data.user_id : 'unavailable';
  
    // Map values into context object passed into Lambda function, if data is present
    if (data) {
      user.email = data.email;
      user.email_verified = data.email_verified;
      authResponse.context = user;
    }
  
    if (effect && resource) {
      const policyDocument = {};
      policyDocument.Version = '2012-10-17';
      policyDocument.Statement = [];
      const statementOne = {};
      statementOne.Action = 'execute-api:Invoke';
      statementOne.Effect = effect;
      statementOne.Resource = resource;
      policyDocument.Statement[0] = statementOne;
      authResponse.policyDocument = policyDocument;
    }
  
    return authResponse;
  }
  
exports.handler = async (event, context) => {

    console.log("Incoming request");
    console.log("event");
    console.log(event);
    console.log("context:");
    console.log(context);

    try {
  
      // Return from function if no authorizationToken present in header
      // context.fail('Unauthorized') will trigger API Gateway to return 401 response
      if (!event.authorizationToken) {
        return context.fail('Unauthorized');
      }
  
      // If auhorizationToken is present, split on space looking for format 'Bearer <token value>'
      const tokenParts = event.authorizationToken.split(' ');
      const tokenValue = tokenParts[1];
  
      // Return from function if authorization header is not formatted properly
      if (!(tokenParts[0].toLowerCase() === 'bearer' && tokenValue)) {
        return context.fail('Unauthorized');
      }
  
      // Prepare for validating Firebase JWT token by initializing SDK
      initializeFirebaseAdminSdk();
  
      // Call the firebase-admin provided token verification function with
      // the token provided by the client
      // Generate Allow on successful validation, otherwise catch the error and Deny the request
      let resp = await firebaseAdmin.auth().verifyIdToken(tokenValue);
      return generateIamPolicy('Allow', event.methodArn, resp);
  
    } catch (err) {
      return generateIamPolicy('Deny', event.methodArn, null);
    }
}



