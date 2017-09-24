import _ from 'lodash';

const generateRages = (length, size) => {
  if (size <= 0) return [];
  const startIndexs = _.range(0, length, size);
  const endIndexs = _.map(startIndexs, x => (x + (size - 1)));
  endIndexs[endIndexs.length - 1] = length - 1;
  const indexs = _.range(1, startIndexs.length + 1);
  return _.zipWith(indexs, startIndexs, endIndexs, (x, y, z) => ({
    number: x,
    range: `bytes=${y}-${z}`,
  }));
};

export {
  generateRages,
};
