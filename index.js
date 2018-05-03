"use strict";

const zookeeper = require('node-zookeeper-client');
const process = require('process');
const path = require('path');


/**
 * 回滚数据存储与获取
 * Created by Galois Zhou on 2018/4/11 09:36
 * @since 1.9.3
 */
class CuratorCrud {
  constructor() {
    this.host = process.env.ZOOKEEPER_CURATOR_HOST || 'dev.local';
    this.port = process.env.ZOOKEEPER_CURATOR_PORT || 2181;
    this.timeout = process.env.ZOOKEEPER_CURATOR_TIMEOUT || 10000;
    this.rootPath = process.env.ZOOKEEPER_CURATOR_ROOT_PATH || '/metadata';

    this.pool = [];
    this.poolSize = process.env.ZOOKEEPER_CURATOR_SIZE || 10;
    this.prepareConnect = {};
    this.usedClinetIndex = {};

    for (let i = 0; i < Math.ceil(this.poolSize / 3); i++) {
      this.connect();
    }
  }

  /**
   * 创建节点
   * @param dataPath
   * @returns {Promise<void>}
   */
  async create(dataPath, data) {
    let client = await this.getUsableClient();
    try {
      await this._create(client, dataPath, data);
      return true;
    } catch (e) {
      console.error('create error: ', e);
      return false;
    }
  }

  /**
   * 设置数据
   * @param dataPath
   * @param data
   * @returns {Promise<any>}
   * @since 1.9.3
   */
  async setData(dataPath, data) {
    let client = await this.getUsableClient();
    try {
      await this._createParentPath(client, dataPath);
    } catch (e) {}
    try {
      let stat = await this._setData(client, dataPath, data);
      return stat;
    } catch (e) {
      console.error('setData error: ', e);
      return false;
    }
  }

  /**
   * 获取数据
   * @param dataPath
   * @returns {Promise<*>}
   * @since 1.9.3
   */
  async getData(dataPath) {
    try {
      let client = await this.getUsableClient();
      let data = await this._getData(client, dataPath);
      return data;
    } catch (e) {
      console.error('getData error: ', e);
    }
    return null;
  }

  // TODO 应该路径一直向上找，是否存在，不存在就创建，存在就可以返回了。
  async _createParentPath(client, dataPath) {
    let paths = dataPath.split('/');
    let parentPath = '/';
    for (let i = 1; i < paths.length; i++) {
      parentPath = path.join(parentPath, paths[i]);
      try {
        await this._create(client, parentPath);
      } catch (e) {}
    }
  }

  _create(client, dataPath, data) {
    return new Promise((resolve, reject) => {
      data = data || '';
      if (typeof data === 'object') {
        data = JSON.stringify(data);
      }
      // console.log('--------------create ', path.join(this.rootPath, dataPath));
      client.create(path.join(this.rootPath, dataPath), new Buffer(data), zookeeper.CreateMode.PERSISTENT, (error) => {
        if (error && (!error.stack || error.stack.indexOf('NODE_EXISTS') == -1)) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  _setData(client, dataPath, data) {
    return new Promise((resolve, reject) => {
      if (!client) {
        console.error('######################### zookeeper no usable client......: ');
        return reject();
      }
      if (typeof data === 'object') {
        data = JSON.stringify(data);
      }
      // console.log('--------------set data ', path.join(this.rootPath, dataPath));
      client.setData(path.join(this.rootPath, dataPath), new Buffer(data), -1, (error, stat) => {
        if (error) {
          console.error('######################### zookeeper set data error: ', error);
          reject(error);
        } else {
          resolve(stat);
        }
      });
    });
  }

  _getData(client, dataPath) {
    return new Promise((resolve, reject) => {
      if (!client) {
        console.error('######################### zookeeper no usable client......: ');
        return reject();
      }
      client.getData(path.join(this.rootPath, dataPath), (error, data, stat) => {
        if (error) {
          console.error('######################### zookeeper get data error: ', error);
          reject(error);
        } else {
          resolve(data);
        }
      });
    });
  }


  /**
   * 获取一个可用的连接
   * @returns {*}
   * @since 1.9.3
   */
  async getUsableClient() {
    let index = -1;
    for (let i = 0; i < this.poolSize; i++) {
      if (this.pool[i] && (!this.usedClinetIndex[i] || Date.now() - this.usedClinetIndex[i] > 500)) {
        index = i;
        break;
      }
    }
    if (index == -1) {
      try {
        let result = await this.connect();
        if (result) {
          index = result.index;
        }
      } catch (e) {
        // console.error('####################### getUsableClient create connect error: ', e);
      }
    }
    if (index == -1) {
      let randomIndex = Math.floor(Math.random() * this.poolSize);
      for (let j = 0; j < this.poolSize; j++) {
        let i = randomIndex - j;
        if (i >= 0 && i < this.poolSize && this.pool[i]) {
          index = i;
          break;
        }
        i = randomIndex + j;
        if (i >= 0 && i < this.poolSize && this.pool[i]) {
          index = i;
          break;
        }
      }
      // console.log('============================= : ', index);
    }
    if (index != -1) {
      console.log('--------------use : ', index);
      this.usedClinetIndex[index] = Date.now();
      return this.pool[index];
    } else {
      console.log('-------------- no client ......');
      return null;
    }
  }

  /**
   * 获取一个空闲的位置
   * @returns {number} -1 表示没有空闲位置
   * @since 1.9.3
   */
  getEmptyIndex() {
    let index = -1;
    for (let i = 0; i < this.poolSize; i++) {
      // console.log(!this.pool[i], !this.prepareConnect[i], !this.prepareConnect[i] || Date.now() - this.prepareConnect[i] > 10000);
      if (!this.pool[i] && (!this.prepareConnect[i] || Date.now() - this.prepareConnect[i] > 10000)) {
        index = i;
        break;
      }
    }
    return index;
  }

  /**
   * 创建一个连接
   * @returns {Promise<any>}
   * @since 1.9.3
   */
  connect() {
    return new Promise((resolve, reject) => {

      let index = this.getEmptyIndex();
      if (index == -1) { // 已经超过了连接池的总数
        return reject('no empty index!');
      }
      this.prepareConnect[index] = Date.now();
      this.close(this.pool[index]);
      this.pool[index] = null;

      let client = zookeeper.createClient(this.host + ':' + this.port, {
        sessionTimeout: this.timeout,
        spinDelay: 1000,
        retries: 10
      });

      if (!client) {
        return reject('create client failed!');
      }
      this._listen(index, client).then(data => {
        resolve(data);
      }).catch(e => {
        reject(e);
      });
      client.connect();
    });
  }

  _listen(index, client) {
    return new Promise((resolve, reject) => {
      let isReturn = false;
      client.once('connected', () => {
        console.log('---------------------------connected: ', index);
        this._create(client, '').then(() => {
          if (!isReturn) {
            isReturn = true;
            // if (!this.pool[index]) {
            this.pool[index] = client;
            // }
            resolve({
              index: index,
              client: client,
            });
          } else {
            this.close(client);
          }
        }).catch(error => {
          reject(error);
          this.close(client);
        });
      });
      client.once('expired', () => {
        console.log('---------------------------expired: ', index);
        this.pool[index] = null;
        this.close(client);
      });
      client.once('authenticationFailed', () => {
        console.log('---------------------------authenticationFailed: ', index);
        this.pool[index] = null;
        this.close(client);
      });
      client.once('disconnected', () => {
        console.log('---------------------------disconnected: ', index);
        this.pool[index] = null;
        this.close(client);
      });

      setTimeout(() => {
        if (!isReturn) {
          isReturn = true;
          reject('TIME_OUT');
          this.close(client);
        }
      }, this.timeout);
    });
  }

  /**
   * 关闭某个客户端
   * @param client
   * @since 1.9.3
   */
  close(client) {
    client && client.close();
  }

  // 所有可监听事件
  // client && client.once('connectedReadOnly', () => {});
  // client && client.once('connected', () => {});
  // client && client.once('expired', () => {});
  // client && client.once('authenticationFailed', () => {});
  // client && client.once('disconnected', () => {});

}

var curatorCrud = new CuratorCrud();


module.exports = curatorCrud;