/**
 * @template { import('./queue.connector').QueueConnector } T
 * @template { {} } [J = object]
 */
class CronQueue {
  /**
   * @param { object } props
   * @param { import('./queue.api').QueueApi<T> } props.api
   * @param { string } props.type
   * @param { object } props.consumer
   * @param { (id: string | number, json: J) => Promise<void> } props.consumer.handler
   * @param { number } props.consumer.intervalSec
   * @param { object } [props.consumerFailed]
   * @param { (id: string | number, json: J) => Promise<boolean> } props.consumerFailed.handler
   * @param { number } props.consumerFailed.intervalSec
   * @param { object } [props.handleHanged]
   * @param { 'drop' | 'restart' } props.handleHanged.type
   * @param { number } props.handleHanged.intervalMin
   */
  constructor({ api, type, consumer, consumerFailed, handleHanged }) {
    /** @private */
    this._type = type;
    /** @private */
    this._api = api;
    /** @private */
    this._consumer = {
      intervalMs: consumer.intervalSec * 1000,
      handler: consumer.handler,
      /** @type { null | NodeJS.Timer } */
      interval: null,
      isBlocked: false,
    };
    this._consumerFailed = consumerFailed
      ? {
          intervalMs: consumerFailed.intervalSec * 1000,
          handler: consumerFailed.handler,
          /** @type { null | NodeJS.Timer } */
          interval: null,
          isBlocked: false,
        }
      : undefined;
    /** @private */
    this._handleHanged = handleHanged
      ? {
          type: handleHanged.type,
          intervalMs: handleHanged.intervalMin * 60 * 1000,
          /** @type { null | NodeJS.Timer } */
          interval: null,
          isBlocked: false,
        }
      : undefined;

    this._validateFields();
    this._runItervals();
  }

  /**
   * @private
   */
  _validateFields() {}

  /**
   * @private
   */
  _runItervals() {
    this._runIntervalPooling(this._consumer, (pooling) =>
      this._intervalPoolingHandlerConsume(pooling)
    );

    if (this._consumerFailed) {
      this._runIntervalPooling(this._consumerFailed, (pooling) =>
        this._intervalPoolingHandlerConsumeFailed(pooling)
      );
    }

    if (this._handleHanged) {
      this._runIntervalPooling(this._handleHanged, async ({ type }) => {
        if (type === "drop") {
          await this._api.dropHangedTasks({ type: this._type });
        } else {
          await this._api.restartHangedTasks({ type: this._type });
        }
      });
    }
  }

  /**
   * @template { Pick<typeof this._consumer, 'isBlocked' | 'interval' | 'intervalMs'> } T
   * @param { T } intervalPooling
   * @param { (pooling: T) => Promise<void> } runner
   * @private
   */
  _runIntervalPooling(intervalPooling, runner) {
    if (!intervalPooling.interval) {
      intervalPooling.isBlocked = false;
      intervalPooling.interval = setInterval(async () => {
        if (intervalPooling.isBlocked) {
          return;
        }
        try {
          intervalPooling.isBlocked = true;
          await runner(intervalPooling);
        } catch {
        } finally {
          intervalPooling.isBlocked = false;
        }
      }, intervalPooling.intervalMs);
    }
  }

  /**
   * @param { typeof this._consumer } intervalPooling
   * @private
   */
  async _intervalPoolingHandlerConsume(intervalPooling) {
    /** @type { Awaited<ReturnType<typeof this._api.consumeTask<J>>> } */
    const task = await this._api.consumeTask({
      type: this._type,
    });
    if (!task) {
      return;
    }
    const { id, json } = task;
    try {
      await intervalPooling.handler(id, json);
      await this._api.completeSuccessTask({ id });
    } catch (error) {
      await this._api.completeFailedTask({
        id,
        // @ts-ignore
        errorText: error?.message,
      });
    }
  }

  /**
   * @param { Exclude<typeof this._consumerFailed, undefined> } intervalPooling
   * @private
   */
  async _intervalPoolingHandlerConsumeFailed(intervalPooling) {
    /** @type { Awaited<ReturnType<typeof this._api.consumeFailedTask<J>>> } */
    const task = await this._api.consumeFailedTask({
      type: this._type,
    });
    if (!task) {
      return;
    }
    const { id, json, errorText } = task;
    try {
      const isSuccess = await intervalPooling.handler(id, json);
      if (isSuccess) {
        await this._api.completeSuccessTask({ id });
      } else {
        await this._api.completeFailedTask({ id, errorText });
      }
    } catch (error) {
      await this._api.completeFailedFatalTask({
        id,
        // @ts-ignore
        errorText: error?.message,
      });
    }
  }
}

module.exports = { CronQueue };
