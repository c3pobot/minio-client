'use strict'
const Minio = require('minio')
const client = new Minio.Client({
  endPoint: process.env.MINIO_SCV_URL,
  port: process.env.MINIO_PORT,
  useSSL: false,
  accessKey: process.env.MINIO_ACCESSKEY,
  secretKey: process.env.MINIO_SECRETKEY
})
const getObject = (bucket, key)=>{
  return new Promise((resolve, reject)=>{
    try{
      let miniData
      client.getObject(bucket, key, (err, dataStream)=>{
        if(err) reject(err)
        dataStream.on('data', (chunk)=>{
          if(!miniData){
            miniData = chunk
          }else{
            miniData += chunk
          }
        })
        dataStream.on('end', ()=>{
          resolve(miniData)
        })
        dataStream.on('error', (err)=>{
          reject(err)
        })
      })
    }catch(e){
      reject(e)
    }
  })
}
module.exports.putJSON = async(bucket, path, fileName, data, expireTime)=>{
  try{
    let metadata = { 'Content-Type': 'application/json' }
    if(expireTime) metadata.ttl = Date.now() + expireTime * 1000
    let key = ''
    if(path) key += `${path}/`
    key += `${fileName}.json`
    let result = client.putObject(bucket, key, JSON.stringify(data), metadata)
    return result?.etag
  }catch(e){
    throw(e)
  }
}
module.exports.getJSON = async(bucket, path, fileName)=>{
  try{
    let key = ''
    if(path) key += `${path}/`
    key += `${fileName}.json`
    let result = await getObject(bucket, key)
    if(result) return JSON.parse(result)
  }catch(e){
    throw(e)
  }
}
