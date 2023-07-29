const { QueueConnector } = require("./queue.connector");

/**
 * MSSQL +
 * POSTGRES +
 */

const STATUS_NEW = 0;
const STATUS_PROGRESS = 1;
const STATUS_COMPLETED = 2;
const STATUS_ERROR = 3;
const STATUS_FATAL_ERROR = 4;

const TABLE_NAME = "task";
const SCHEMA_NAME = "qc";
const PNAME = `${SCHEMA_NAME}_${TABLE_NAME}`;
const CNAME = `${SCHEMA_NAME}.${TABLE_NAME}`;

/**
 * @param { object } props
 * @param { number } [props.handlerDelayedTimeoutForError]
 * @param { number } [props.handlerDelayedHangedLag]
 */
const driverBindingBuilder = ({
  handlerDelayedTimeoutForError = 5,
  handlerDelayedHangedLag = -1,
}) => {
  return {
    mssql: {
      name: "mssql",
      delayedToForError: `DATEADD(minute, ${handlerDelayedTimeoutForError} * attempt, CURRENT_TIMESTAMP)`,
      delayedHangedLag: `DATEADD(hour, ${handlerDelayedHangedLag}, CURRENT_TIMESTAMP)`,
    },
    otherwise: {
      name: "otherwise",
      delayedToForError: `CURRENT_TIMESTAMP + INTERVAL '${handlerDelayedTimeoutForError} minutes' * attempt`,
      delayedHangedLag: `CURRENT_TIMESTAMP + INTERVAL '${handlerDelayedHangedLag} hour'`,
    },
  };
};

class QueueConnectorKnex extends QueueConnector {
  /**
   * @param { object } props
   * @param { import('knex').Knex } props.knex
   * @param { Parameters<typeof driverBindingBuilder>[0] } [props.options]
   */
  constructor({ knex, options }) {
    super();
    /** @private */
    this._knex = knex;

    const DRIVER_BINDING = driverBindingBuilder(options ?? {});

    const driverName = knex.client.driverName;
    /**
     * @type { (typeof DRIVER_BINDING)[keyof typeof DRIVER_BINDING] }
     * @private
     */
    this._driverBinding =
      // @ts-ignore
      DRIVER_BINDING[driverName] ?? DRIVER_BINDING.otherwise;
  }

  async init() {
    const self = this;
    const knex = this._knex;
    await knex.schema
      .withSchema(SCHEMA_NAME)
      .hasTable(TABLE_NAME)
      .then(async function (exists) {
        if (!exists) {
          await knex.schema
            .withSchema(SCHEMA_NAME)
            .createTable(TABLE_NAME, (table) => {
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
            `create index ${PNAME}__status__delayed_to__idx on ${CNAME} (status, delayed_to)`
          );
          await knex.schema.raw(
            `create index ${PNAME}__updated__idx on ${CNAME} (updated)`
          );
          if (self._isDriver("otherwise")) {
            await knex.schema.raw(
              `create index ${PNAME}__type__idx on ${CNAME} (type)`
            );
          }
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
    const [{ id }] = await knex(CNAME).insert(
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
    const value = await knex(CNAME)
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
          .from(CNAME)
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
    const value = await knex(CNAME)
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
          .from(CNAME)
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
    await knex(CNAME).where("id", "=", id).update({
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
    await knex(CNAME)
      .where("id", "=", id)
      .update({
        status: STATUS_ERROR,
        end_time: knex.fn.now(),
        delayed_to: knex.raw(this._driverBinding.delayedToForError),
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
    await knex(CNAME).where("id", "=", id).update({
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
    await knex(CNAME)
      .where({
        status: STATUS_PROGRESS,
        type,
      })
      .andWhere("updated", "<", knex.raw(this._driverBinding.delayedHangedLag))
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
    await knex(CNAME)
      .where({
        status: STATUS_PROGRESS,
        type,
      })
      .andWhere("updated", "<", knex.raw(this._driverBinding.delayedHangedLag))
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

  /**
   * @param  { ...string } drivers
   * @private
   */
  _isDriver(...drivers) {
    const isDriverMatch = drivers.find(
      (driver) => this._driverBinding.name === driver
    );
    return typeof isDriverMatch == "string";
  }
}

module.exports = { QueueConnectorKnex };
