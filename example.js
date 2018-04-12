"use strict";

const curatorCrud = require('./index');

setTimeout(async () => {

console.log(curatorCrud)

  console.log('set : ', '/d/c/b/a', {a: 1});
  await curatorCrud.setData('/d/c/b/a', {a: 1});

  let data = await curatorCrud.getData('/d/c/b/a');
  console.log('get: ', '/d/c/b/a', data.toString());

  process.exit();

}, 5000);


