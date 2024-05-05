/*
Starts a javascript client to interact with a FHIR test server
Added the ability to retreive a resource from the server and upload that resource to mongo, spark and mysql server
*/

//Import mongoclient and Serverapiversion to make a client to interact with my mongo server
const { MongoClient, ServerApiVersion } = require("mongodb");

//Mongo server uri
const uri = "mongodb://127.0.0.1:27017";

// Create a MongoClient and use the optional serverApi configuration to avoid errors or restricted behaviors
const mongoClient = new MongoClient(uri,  {
        serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: true,
        }
    }
);


//Prompt for user input
const prompt = require("prompt-sync")({ sigint: true });

//Import fhir.js
const FHIR = require('fhir.js');

//Define APISIX key auth 
const apikey = 'Thebigblue12!';

//Setting my endpoint to my APISIX deployment url which will process through route and upstream to fhir endpoint
const client = {
  baseUrl: 'http://127.0.0.1:9080',
  credentials: 'same-origin',
  headers: {
    'apikey': apikey
  }  
};


//Client variable and start client
const myClient = FHIR(client);

//Input
console.log("1. Create\n2. Read\n3. Update\n4. Delete\n5. Send to mongo and spark\n");
var choice = prompt("choice: ");

//Quick if-else
if (choice == 1)
{
  //Get info for the resource
  const patientName = prompt("Enter name: ")

  //Define patient resource
  const patientResource = {
    resource: {
      resourceType: 'Patient',
      name: [
      {
        use: "official",
        given: [patientName],
      }
    ],
    gender: 'male',
    birthDate: '1980-01-01',
    address: [
    {
      use: 'home',
      line: ['123 Main St'],
      city: 'Anytown',
      state: 'CA',
      postalCode: '12345',
      country: 'USA'
    }
    ],
    telecom: [
    {
      system: 'phone',
      value: '555-123-4567'
    }
    ],
  } 
};
  //Create a patient resource on the test server via POST
  myClient.create(patientResource)
    //Print error or server response
    .then(response => {
      console.log('Patient resource created:', response);
    })
    .catch(error => {
      console.error('Error creating patient resource:', error);
    });
  }
else if (choice == 2)
{
  var searchId = prompt('Who to search for: ')
  //Get request
  myClient.read({type: 'Patient', id: searchId})
  //Print error or server response
  .then(response => {
    console.log('Patient resource found:', response);
  })
  .catch(error => {
    console.error('Error finding patient resource:', error);
  });
}
else if (choice == 3)
{
  var searchId = prompt("Whose name to update: ");
  var newName = prompt("New name: ");

  //Make a GET request and update the resource via PUT
  myClient.read({type: 'Patient', id: searchId})
    .then(response => {
      const updatedResource = response.data;

      updatedResource.name[0].given = [newName];
          
      //Update the modified resource
      return myClient.update({resource: updatedResource});
    })
    .then(response => {
      console.log('Resourse updated', response);
    })
    .catch(error => {
      console.error('Error updating resource', error);
    });
}
else if (choice == 4)
{
  var deleteId = prompt('Who do you want to delete: ')
  //DELETE request
  myClient.delete({type: 'Patient', id: deleteId})
    .then(response => {
      console.log('Patient resource deleted:', response);
    })
    .catch(error => {
      console.error('Error deleting patient resource:', error);
    });
}
else
{
  var searchId = prompt('Who to send to mongo: ')
  //GET request to the fhir server with an asynchronous function that will upload the fetched resource to my mongo server
  myClient.read({type: 'Patient', id: searchId})
  //If I use .then() I wont need to concatenate and parse the data chunks
    .then(response => {
      const patient = response.data;
      //Functions
      async function run() {
        try {
          // Connect the client to the server, must resolve before creating the db and collection
          await mongoClient.connect();

          const fhirDb = mongoClient.db("fhir-resource");
          const fhirColl = fhirDb.collection("Patients");

          //Insert the JSON payload to the collection, will wait for insertOne to resolve before printing to the console
          const result = await fhirColl.insertOne(patient);
          console.log(
            `A document was inserted with the _id: ${result.insertedId}`,
          );
        } finally {
          // Ensures that the client will close when you finish/error
          await mongoClient.close();
        }
      }
      run().catch(console.dir);
    })
    .catch(error => {
      console.error('Error finding resource', error);
    }); 
}




