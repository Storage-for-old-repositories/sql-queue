const NOT_IMPLEMENTED_TEXT = "Not implemented";

class QueueConnector {
  async init() {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string } props.type
   * @param { object } props.json
   * @returns { Promise<{ id: string | number }> }
   */
  async insertTask(props) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @template { {} } [T = object]
   * @param { object } props
   * @param { string } props.type
   * @returns { Promise<null | { id: string | number, json: T }> }
   */
  async consumeTask(props) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @template { {} } [T = object]
   * @param { object } props
   * @param { string } props.type
   * @returns { Promise<null | { id: string | number, json: T, errorText?: string }> }
   */
  async consumeFailedTask(props) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string | number } props.id
   */
  async completeSuccessTask({ id }) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string | number } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedTask({ id, errorText }) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string | number } props.id
   * @param { string } [props.errorText]
   */
  async completeFailedFatalTask({ id, errorText }) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async dropHangedTasks({ type }) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }

  /**
   * @param { object } props
   * @param { string } props.type
   */
  async restartHangedTasks({ type }) {
    throw new Error(NOT_IMPLEMENTED_TEXT);
  }
}

module.exports = { QueueConnector };
