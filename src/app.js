import _ from 'lodash';
import rp from 'request-promise-native';

// s3 structure
// config
//   job
//     job1.json
//     job2.json
//   subjob
//     job1
//       c1.json
//       c2.json
//       ....
//     job2
//       d1.json
//       d2.json
//       ...
//   etags
//     job1
//       etag1.json
//       etag2.json
//       ...
//     job2
//       etag1.json
//       etag2.json
//       ....
//
// files
//   video1.mp4
//   video2.mp4
//   ....

module.exports.trigger_job = (event, context, cb) => {

  //1. get job config from request
  //{
  //  url: http://.......
  //  name: video.mp4
  //}
  //
  //2. get file content length
  //  Range request
  //
  //3. generate sub job config
  // {
  //   number: 1
  //   url: http://........
  //   range: 0-100/3000
  //   name: video.mp4
  //   etagFile: etags/etag1.json
  //   bucket: video
  // }
  //
  // 4. generate job monitor config
  // { 
  //   bucket: video
  //   name: video3.mp4
  //   parts: [
  //     {id:1,etagFile: etags/etag1.jsob },{id:2,etagFile: etags/etag2.jsob } 
  //   ]
  // }

  const jobBucket = _.get(event, 'stageVariables.job_bucket');
  const fileBucket = _.get(event, 'stageVariables.file_bucket');
  const jobPrefix = _.get(event, 'stageVariables.job_prefix');
  const subjobPrefix = _.get(event, 'stageVariables.subjob_prefix');
  const etagPrefix = _.get(event, 'stageVariables.etag_prefix');
  const jobConfig = JSON.parse(event.body);
  console.log('request body');
  console.log(jobConfig);

  if (!_.has(jobConfig, 'url')) {
    cb(null, { statusCode: 400, body: 'can not find download url' });
    console.log('can not find download url');
    return false;
  }

  if (!_.has(jobConfig, 'name')) {
    cb(null, { statusCode: 400, body: 'can not find file name' });
    console.log('can not find file name');
    return false;
  }

  const option = {
    method: 'GET',
    jobConfig.url,
    headers: {
      Range: 'bytes=0-',
    },
  };

  rp(option).then((events) => {
    console.log(events);
    const length = parseInt(events.headers['content-length']);
    const ranges = generateRages(length,1024*1024*2)
    createMultipartUpload(file_bucket,name).then((info)=>{
      const configs = ranges.map(x=>{
        return {
          number: x.number,
          url: jobConfig.url,
          range: x.range,
          name: jobConfig.name,
          etagFile: etagPrefix+'etag'+x.number+'.json',
          bucket: fileBucket,
          uploadId: info.uploadId
        }
      }) 

      const monitorConfig = {
        bucket: fileBucket,
        name: jobConfig.name,
        parts: configs.map(x=>{
          return {
            x.number,
            x.etagFile
          }
        })
      }

      return saveJobConfig(configs,monitorConfig,jobBucket,jobPrefix,subjobPrefix)
    })
  }).catch((err)=>{
    console.log("Failed to create job for "+jobConfig.url+" "+ err)
    cb(null, { statusCode: 500, body: err });
  });

};

module.exports.download = (event, context, cb) => {
  // 1. read job config
  // {
  //   number: 1
  //   url: http://........
  //   range: 0-100/3000
  //   name: video.mp4
  //   etagFile: etags/etag1.json
  //   bucket: video
  // }
  //
  // 2. range request download data
  // 3. upload multipart data to s3
  // 4. save etag to etagFile
  // 4. delete job config if success or rewrite the config to trigger another job
}

module.exports.check_then_complete = (event, context, cb) => {
  // 1. get all the job monitor config
  // 2. if all etag file exist for this job
  //    then
  //       send complete multipart upload to s3 then delete this config
  //    else
  //       ignore this job
}
