require("dotenv").config();
const express = require("express");
const { MongoClient } = require("mongodb");

const app = express();

// Connection URL
const url = "mongodb://localhost:27017";
const client = new MongoClient(url);

// Database Name
const dbName = "logQueryTest";

app.use(express.json());

async function main() {
  // Use connect method to connect to the server
  await client.connect();
  console.log("Connected successfully to server");
  const db = client.db(dbName);

  // if needed test the GET api by inserting the data required data into the collections

  const customers = db.collection("customers");
  const customerLogs = db.collection("customerLogs");
  const locations = db.collection("locations");

  app.get("/getCustomerLogs", async (req, res) => {
    //The Date format should be ISO String based
    //"2019-10-05T14:48:00.000Z"
    const startDate = req.body.startDate; // from date
    const endDate = req.body.endDate; // to date

    const locationID = req.body.locationID;

    try {
      const customerLogs = await db.customerLog.aggregate([
        {
          // gets all the customer logs between the given dates
          $match: {
            date: {
              $gte: ISODate(startDate),
              $lte: ISODate(endDate),
            },
          },
        },
        {
          // gets all the customers of the customerId from the logs
          $lookup: {
            from: "customer",
            localField: "customerId",
            foreignField: "_id",
            as: "customerLog",
          },
        },
        {
          // extracts the object from the lookup array
          $unwind: "$customerLog",
        },
        {
          //gets all the logs where the required location matches
          $match: {
            "customerLog.locationId": locationID,
          },
        },
        {
          // excludes the logs from the final object
          $project: {
            customerLog: 0,
          },
        },
        {
          // groups the logs by customer IDs
          $group: {
            _id: "$customerId",
            logs: {
              $push: "$$ROOT",
            },
          },
        },
      ]);
      res.status(200).json({ customerLogs });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  });

  app.listen(8000, () => {
    console.log("Server Listening at port 8000...");
  });

  return "Connection to Database established";
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());
