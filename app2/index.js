// this is kafka consumer
const express = require("express");
const kafka = require("kafka-node");
const mongoose = require("mongoose");

const app = express();
app.use(express.json());

const areFine = () => {
  // connect to the db
  mongoose.connect(process.env.MONGO_URL);
  // create a user model
  const User = new mongoose.model("user", {
    name: String,
    email: String,
    password: String,
  });
  // create a post model
  const Posts = new mongoose.model("posts", {
    title: String,
    body: String,
  });
  // connect to kafka client
  const client = new kafka.KafkaClient({
    kafkaHost: process.env.KAFKA_BOOTSTRAP_SERVERS,
  });
  // initiate a consumer
  const consumer = new kafka.Consumer(
    client,
    // you can use multiple topics here
    // subscribe to multiple topics
    [{ topic: process.env.KAFKA_TOPIC1 }, { topic: process.env.KAFKA_TOPIC2 }],
    { autoCommit: false }
  );
  // listen for messages in the selected kafka topic
  consumer.on("message", async (message) => {
    // save the received msg in mongo dc
    // app2 is subscribed to 2 topics, all depends on the message topic :)
    if (message.topic === "topic1") {
      const user = await new User(JSON.parse(message.value));
      await user.save();
    } else if (message.topic === "topic2") {
      const post = await new Posts(JSON.parse(message.value));
      await post.save();
    }
  });

  consumer.on("error", (err) => console.log(err));
};

// wait some time so the servers starts : this is an assumption
setTimeout(areFine, 30000);

app.listen(process.env.PORT);
