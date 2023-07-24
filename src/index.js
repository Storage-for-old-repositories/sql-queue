const knex = require("knex").knex({
  client: "pg",
  connection: {
    host: "localhost",
    user: "myuser",
    password: "mypassword",
    database: "mydatabase",
  },
  // debug: true,
});

const { QueueConnectorKnex } = require("./queue.knex.connector");
const { QueueApi } = require("./queue.api");
const { CronQueue } = require("./cron.queue");

const queue = new QueueApi(new QueueConnectorKnex({ knex }));

async function boost() {
  await queue.init();

  await queue.dropHangedTasks({ type: "test" });

  for (let i = 0; i < 20; ++i) {
    await queue.insertTask({
      type: "test",
      json: {
        id: Math.floor(Math.random() * 1000),
      },
    });
  }

  new CronQueue({
    api: queue,
    type: "test",
    consumer: {
      intervalSec: 1,
      handler: async (id, json) => {
        console.log("pool<normal>", [id, json]);
        if (Math.random() > 0.5) {
          throw new Error("Test<pool<normal>>");
        }
      },
    },
    consumerFailed: {
      intervalSec: 5,
      handler: async (id, json) => {
        console.log("pool<failed>", [id, json]);
        const choose = Math.random();
        if (choose > 0.66) {
          throw new Error("Test<pool<failed>>");
        }
        return choose > 0.33;
      },
    },
    handleHanged: {
      intervalMin: 1,
      type: "restart",
    },
  });

  // knex.destroy();
}

boost();
