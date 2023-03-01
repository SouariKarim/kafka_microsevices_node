// this is the producer wich uses postgresql
const express = require("express");
const kafka = require("kafka-node");
const sequelize = require("sequelize");
const app = express();
app.use(express.json());

const areFine = async () => {
  // initialize the sequelize orm
  const db = new sequelize(process.env.POSTGRES_URL);
  // create a user table
  const Posts = db.define("posts", {
    title: sequelize.STRING,
    body: sequelize.STRING,
  });
  // forse the table creation in the db
  db.sync({ force: true });
  // initialize the connection to kafka
  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  // initialize the producer
  const producer = new kafka.Producer(client);
  producer.on("ready", () => {
    app.post("/app3", (req, res) => {
      // when making a post request make the producer send to kafka topic a message
      producer.send(
        [
          {
            // the name of kafka topic
            topic: process.env.KAFKA_TOPIC,
            // the message to send to kafka topic
            messages: JSON.stringify(req.body),
          },
        ],
        async (err, data) => {
          if (err) console.log(err);
          else {
            // after sending the message to kafka topic do this
            // create a user
            await Posts.create(req.body);
            res.send(req.body);
          }
        }
      );
    });
  });
};

//  wait for some time so the servers starts : this is an assumption
setTimeout(areFine, 10000);

app.listen(process.env.PORT);
