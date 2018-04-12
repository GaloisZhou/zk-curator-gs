"use strict";

const zookeeper = require('node-zookeeper-client');
const process   = require('process');
const path      = require('path');


/**
 * zookeeper set and get data
 * Created by Galois Zhou on 2018/4/11 09:36
 */
class CuratorCrud {
  constructor() {
    this.host     = process.env.ZOOKEEPER_CURATOR_HOST || 'dev.local';
    this.port     = process.env.ZOOKEEPER_CURATOR_PORT || 2181;
    this.timeout  = process.env.ZOOKEEPER_CURATOR_TIMEOUT || 10000;
    this.rootPath = process.env.ZOOKEEPER_CURATOR_ROOT_PATH || '/metadata';

    this.pool            = [];
    this.poolSize        = process.env.ZOOKEEPER_CURATOR_SIZE || 10;
    this.prepareConnect  = {};
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
    await this._create(client, dataPath, data);
  }

  /**
   * 设置数据
   * @param dataPath
   * @param data
   * @returns {Promise<any>}
   */
  async setData(dataPath, data) {
    let client = await this.getUsableClient();
    try {
      await this._createParentPath(client, dataPath);
    } catch (e) {
    }
    let stat = await this._setData(client, dataPath, data);
    return stat;
  }

  /**
   * 获取数据
   * @param dataPath
   * @returns {Promise<*>}
   */
  async getData(dataPath) {
    let client = await this.getUsableClient();
    let data   = await this._getData(client, dataPath);
    return data;
  }

  _setData(client, dataPath, data) {
    return new Promise((resolve, reject) => {
      if (!client) {
        console.error('#########################s zookeeper no usable client......: ');
        return reject();
      }
      if (typeof data === 'object') {
        data = JSON.stringify(data);
      }
      client.setData(path.join(this.rootPath, dataPath), new Buffer(data), -1, (error, stat) => {
          if (error) {
            console.error('#########################s zookeeper set data error: ', error);
            reject(error);
          } else {
            resolve(stat);
          }
        }
      );
    });
  }

  _getData(client, dataPath) {
    return new Promise((resolve, reject) => {
      if (!client) {
        console.error('#########################g zookeeper no usable client......: ');
        return reject();
      }
      client.getData(path.join(this.rootPath, dataPath), (error, data, stat) => {
          if (error) {
            console.error('#########################g zookeeper get data error: ', error);
            reject(error);
          } else {
            resolve(data);
          }
        }
      );
    });
  }

  _create(client, dataPath, data) {
    return new Promise((resolve, reject) => {
      data = data || '';
      if (typeof data === 'object') {
        data = JSON.stringify(data);
      }
      client.create(path.join(this.rootPath, dataPath), new Buffer(data), zookeeper.CreateMode.PERSISTENT, (error) => {
        // dataPath && console.error(error);
        if (error && (!error.stack || error.stack.indexOf('NODE_EXISTS') == -1)) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  async _createParentPath(client, dataPath) {
    let paths      = dataPath.split('/');
    let parentPath = '/';
    for (let i = 1; i < paths.length; i++) {
      parentPath = path.join(parentPath, paths[i]);
      try {
        await this._create(client, parentPath);
      } catch (e) {
      }
    }
  }


  /**
   * 获取一个可用的连接
   * @returns {*}
   */
  async getUsableClient() {
    let index = -1;
    for (let i = 0; i < this.poolSize; i++) {
      if (this.pool[i] && (!this.usedClinetIndex[i] || Date.now() - this.usedClinetIndex[i] > 10000)) {
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
        console.error('####################### getUsableClient create connect error: ', e || '');
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
    }
    if (index != -1) {
      this.usedClinetIndex[index] = Date.now();
      return this.pool[index];
    } else {
      return null;
    }
  }

  /**
   * 获取一个空闲的位置
   * @returns {number} -1 表示没有空闲位置
   */
  getEmptyIndex() {
    let index = -1;
    for (let i = 0; i < this.poolSize; i++) {
      if (!this.pool[i] && (!this.prepareConnect[i] || Date.now() - this.prepareConnect[i] > 10000)) {
        index = i;
        break;
      }
    }
    this.prepareConnect[index] = Date.now();
    return index;
  }

  /**
   * 创建一个连接
   * @returns {Promise<any>}
   */
  connect() {
    return new Promise((resolve, reject) => {
      let client = zookeeper.createClient(this.host + ':' + this.port, { sessionTimeout: this.timeout });

      if (!client) {
        return reject();
      }
      let index = this.getEmptyIndex();
      if (index == -1) { // 已经超过了连接池的总数
        reject();
      }

      client.once('connected', () => {
        this._create(client, '').then(() => {
          if (!this.pool[index]) {
            this.pool[index] = client;
          }
          resolve({index: index, client: client});
        }).catch(error => {
          reject(error);
        });
      });

      client && client.once('expired', () => {
        this.pool[index] = null;
      });
      client && client.once('authenticationFailed', () => {
        this.pool[index] = null;
      });
      client && client.once('disconnected', () => {
        this.pool[index] = null;
      });
      client.connect();
    });
  }

  /**
   * 关闭某个客户端
   * @param client
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

