/**
 * @template { import('./queue.connector').QueueConnector } T
 */
class QueueApi {
  /**
   * @param { T } connector
   */
  constructor(connector) {
    /** @private */
    this._connector = connector;
  }

  async init() {
    await this._connector.init();
  }

  /**
   * @param { object } props
   * @param { string } props.type
   * @param { object } props.json
   */
  async insertTask({ type, json }) {
    return await this._connector.insertTask({
      type,
      json,
    });
  }

  /**
   * @template { {} } [J = object]
   * @param { object } props
   * @param { string } props.type
   */
  async consumeTask({ type }) {
    /** @type { Awaited<ReturnType<typeof this._connector.consumeTask<J>>> } */
    const result = await this._connector.consumeTask({ type });
    return result;
  }

  /**
   * @template { {} } [J = object]
   * @param { object } props
   * @param { string } props.type
   */
  async consumeFailedTask({ type }) {
    /** @type { Awaited<ReturnType<typeof this._connector.consumeFailedTask<J>>> } */
    const result = await this._connector.consumeFailedTask({ type });
    return result;
  }

  /**
   * @param { object } props
   * @param { Parameters<T['completeSuccessTask']>[number]['id'] } props.id
   */
  async completeSuccessTask({ id }) {
    return this._connector.completeSuccessTask({ id });
  }

  /**
   * @param { object } props
   * @param { Parameters<T['completeFailedTask']>[number]['id'] } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedTask({ id, errorText }) {
    return this._connector.completeFailedTask({ id, errorText });
  }

  /**
   * @param { object } props
   * @param { Parameters<T['completeFailedFatalTask']>[number]['id'] } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedFatalTask({ id, errorText }) {
    return this._connector.completeFailedFatalTask({ id, errorText });
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async dropHangedTasks({ type }) {
    return this._connector.dropHangedTasks({ type });
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async restartHangedTasks({ type }) {
    return this._connector.restartHangedTasks({ type });
  }
}

module.exports = { QueueApi };
