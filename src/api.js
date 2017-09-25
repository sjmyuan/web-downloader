import AWS from 'aws-sdk';
import _ from 'lodash';

const readObjectFromS3 = (bucket, key) => {
  const s3 = new AWS.S3();
  const getParams = {
    Bucket: bucket,
    Key: key,
  };
  return s3.getObject(getParams).promise()
    .then(data => JSON.parse(data.Body));
};

const readAllObjectFromS3 = (bucket, keys) =>
  Promise.all(_.map(keys, key => readObjectFromS3(bucket, key)));

const objectExistInS3 = (bucket, key) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.headObject(params).promise()
    .then(() => true).catch(() => false);
};

const writeObjectToS3 = (bucket, key, obj) => {
  const s3 = new AWS.S3();
  const putParams = {
    Bucket: bucket,
    Key: key,
    Body: JSON.stringify(obj),
  };
  return s3.putObject(putParams).promise();
};

const deleteObjectInS3 = (bucket, key) => {
  const s3 = new AWS.S3();
  const putParams = {
    Bucket: bucket,
    Key: key,
  };
  return s3.deleteObject(putParams).promise();
};

const deleteAllObjectInS3 = (bucket, keys) => Promise.all(_.map(keys, key => deleteObjectInS3(bucket, key)));

const listFoldersInS3 = (bucket, prefix) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Delimiter: '/',
    Prefix: prefix,
  };
  return s3.listObjects(params).promise().then(data => _.map(data.CommonPrefixes, ele => ele.Prefix.replace(prefix, '').replace(/\/$/g, '')));
};

const listFilesInS3 = (bucket, prefix) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Delimiter: '/',
    Prefix: prefix,
  };
  return s3.listObjects(params).promise().then(data => _.map(data.Contents, ele => ele.Key));
};

const createMultipartUpload = (bucket, key) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
  };
  return s3.createMultipartUpload(params).promise();
};

const completeMultipartUpload = (bucket, key, uploadId, parts) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
    MultipartUpload: {
      Parts: parts,
    },
    UploadId: uploadId,
  };
  return s3.completeMultipartUpload(params).promise();
};

const abortMultipartUpload = (bucket, key, uploadId) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
    UploadId: uploadId,
  };
  return s3.abortMultipartUpload(params).promise();
};

const uploadPart = (bucket, key, uploadId, partNumber, data) => {
  const s3 = new AWS.S3();
  const params = {
    Bucket: bucket,
    Key: key,
    UploadId: uploadId,
    PartNumber: partNumber,
    Body: data,
  };
  return s3.uploadPart(params).promise();
};

export {
  readObjectFromS3,
  readAllObjectFromS3,
  writeObjectToS3,
  objectExistInS3,
  listFoldersInS3,
  listFilesInS3,
  createMultipartUpload,
  completeMultipartUpload,
  abortMultipartUpload,
  uploadPart,
  deleteObjectInS3,
  deleteAllObjectInS3,
};
