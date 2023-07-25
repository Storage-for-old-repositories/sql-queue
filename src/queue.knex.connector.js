const { QueueConnector } = require("./queue.connector");

const STATUS_NEW = 0;
const STATUS_PROGRESS = 1;
const STATUS_COMPLETED = 2;
const STATUS_ERROR = 3;
const STATUS_FATAL_ERROR = 4;

class QueueConnectorKnex extends QueueConnector {
  /**
   * @param { object } props
   * @param { import('knex').Knex } props.knex
   */
  constructor({ knex }) {
    super();
    /** @private */
    this._knex = knex;
  }

  async init() {
    const knex = this._knex;
    await knex.schema.hasTable("qc_task").then(async function (exists) {
      if (!exists) {
        await knex.schema.createTable("qc_task", (table) => {
          table.bigIncrements("id").primary();
          table.text("type").notNullable();
          table.text("json").notNullable();
          table.integer("status").notNullable().defaultTo(STATUS_NEW);
          table.integer("attempt").notNullable().defaultTo(0);
          table.timestamp("begin_time").nullable();
          table.timestamp("end_time").nullable();
          table.timestamp("delayed_to").nullable();
          table.text("error_text").nullable();
          table.timestamp("created").notNullable().defaultTo(knex.fn.now());
          table.timestamp("updated").notNullable().defaultTo(knex.fn.now());
        });
        await knex.schema.raw(
          "create index qc_task__status__delayed_to__idx on qc_task (status, delayed_to)"
        );
        await knex.schema.raw(
          "create index qc_task__updated__idx on qc_task (updated)"
        );
        await knex.schema.raw(
          "create index qc_task__type__idx on qc_task (type)"
        );
      }
    });
  }

  /**
   * @param { object } props
   * @param { string } props.type
   * @param { object } props.json
   * @returns { Promise<{ id: string }> } id
   */
  async insertTask({ type, json }) {
    const jsonText = JSON.stringify(json);

    const knex = this._knex;
    const [{ id }] = await knex("qc_task").insert(
      {
        type,
        json: jsonText,
      },
      "id"
    );
    return { id };
  }

  /**
   * @template { {} } [T = object]
   * @param { object } props
   * @param { string } props.type
   */
  async consumeTask({ type }) {
    const knex = this._knex;
    const value = await knex('qc_task')
      .update({
        status: STATUS_PROGRESS,
        attempt: knex.raw("attempt + 1"),
        begin_time: knex.fn.now(),
        end_time: null,
        error_text: null,
        delayed_to: null,
        updated: knex.fn.now(),
      })
      .whereIn("id", function () {
        this.select("id")
          .from("qc_task")
          .where("type", "=", type)
          .andWhere("status", "=", STATUS_NEW)
          .limit(1)
          .forUpdate()
          .skipLocked();
      })
      .returning(["id", "json"]);

    if (value.length === 0) {
      return null;
    }

    const [{ id, json }] = value;
    return {
      /** @type { string } */
      id,
      /** @type { T } */
      json: JSON.parse(json),
    };
  }

  /**
   * @template { {} } [T = object]
   * @param { object } props
   * @param { string } props.type
   */
  async consumeFailedTask({ type }) {
    const knex = this._knex;
    const value = await knex("qc_task")
      .update({
        status: STATUS_PROGRESS,
        attempt: knex.raw("attempt + 1"),
        begin_time: knex.fn.now(),
        end_time: null,
        delayed_to: null,
        error_text: null,
        updated: knex.fn.now(),
      })
      .whereIn("id", function () {
        this.select("id")
          .from("qc_task")
          .where("type", "=", type)
          .andWhere("status", "=", STATUS_ERROR)
          .andWhere("delayed_to", "<", knex.fn.now())
          .limit(1)
          .forUpdate()
          .skipLocked();
      })
      .returning(["id", "json", "error_text"]);

    if (value.length === 0) {
      return null;
    }

    const [{ id, json, error_text }] = value;
    return {
      /** @type { string } */
      id,
      /** @type { T } */
      json: JSON.parse(json),
      /** @type { undefined | string } */
      errorText: error_text,
    };
  }

  /**
   * @param { object } props
   * @param { string } props.id
   */
  async completeSuccessTask({ id }) {
    const knex = this._knex;
    await knex("qc_task").where("id", "=", id).update({
      status: STATUS_COMPLETED,
      end_time: knex.fn.now(),
      delayed_to: null,
      error_text: null,
      updated: knex.fn.now(),
    });
  }

  /**
   * @param { object } props
   * @param { string } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedTask({ id, errorText }) {
    const knex = this._knex;
    await knex("qc_task")
      .where("id", "=", id)
      .update({
        status: STATUS_ERROR,
        end_time: knex.fn.now(),
        delayed_to: knex.raw(
          `CURRENT_TIMESTAMP + INTERVAL '5 minutes' * attempt`
        ),
        error_text: errorText,
        updated: knex.fn.now(),
      });
  }

  /**
   * @param { object } props
   * @param { string } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedFatalTask({ id, errorText }) {
    const knex = this._knex;
    await knex("qc_task").where("id", "=", id).update({
      status: STATUS_FATAL_ERROR,
      end_time: knex.fn.now(),
      delayed_to: null,
      error_text: errorText,
      updated: knex.fn.now(),
    });
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async dropHangedTasks({ type }) {
    const knex = this._knex;
    await knex("qc_task")
      .where({
        status: STATUS_PROGRESS,
        type,
      })
      .andWhere(
        "updated",
        "<",
        knex.raw(`CURRENT_TIMESTAMP - INTERVAL '1 hour'`)
      )
      .update({
        status: STATUS_ERROR,
        end_time: knex.fn.now(),
        delayed_to: knex.fn.now(),
        error_text: "hanged",
        updated: knex.fn.now(),
      });
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async restartHangedTasks({ type }) {
    const knex = this._knex;
    await knex("qc_task")
      .where({
        status: STATUS_PROGRESS,
        type,
      })
      .andWhere(
        "updated",
        "<",
        knex.raw(`CURRENT_TIMESTAMP - INTERVAL '1 hour'`)
      )
      .update({
        status: STATUS_NEW,
        attempt: knex.raw("attempt + 1"),
        begin_time: null,
        end_time: null,
        error_text: null,
        delayed_to: null,
        updated: knex.fn.now(),
      });
  }
}

module.exports = { QueueConnectorKnex };
