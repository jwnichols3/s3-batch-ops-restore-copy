//import required modules
const {S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3')
const REGION = 'us-east-1'
const BUCKET = 'cloudtrail-awslogs-040006684403-s9vpuuyo-isengard-do-not-delete'
const client = new S3Client({region: REGION})

//read manifest file from s3 into an array
const getObject = async (bucket, key) => {
     
     const data = await client.send(new GetObjectCommand({Bucket:bucket, Key: key}))
     let temp = null
     data.Body.on('data', data => temp += data.toString('utf8'))
     
     const endFile = new Promise((resolve,reject)=>{
         //expects file to be in the form of Bucket,Key
         data.Body.on('end', ()=> resolve(temp.split('\n').map(i=>i.split(',')[1])))
     })
     const result = await endFile
     return result 
}

//main entry point
(async ()=>{
    //get manifest file from s3
    let manifest = await getObject('bigdatajam', 'trail_keys.csv')
    //remove any empty rows
    manifest = manifest.filter(i => i !== undefined)
    //array to hold batches
    let manifest_parts = []
    //batch size:  Anything over 3500 fails
    const array_slice = 3500
    
    let current_slice = 0
    //break manifest into chunks for batch processing
    while(current_slice < manifest.length) {
        manifest_parts.push(manifest.slice(current_slice, current_slice + array_slice ))
        current_slice += array_slice
    }
    //start a time to track how long the async code runs
    console.time('How long did it take')
    //final results is a summary object that gets built up with each batch using a promise chain
    const finalResults = await manifest_parts.reduce((promiseChain, batch) =>{
        let nextBatch = promiseChain.then( async summary => {
            let result = await Promise.all(batch.map(row => {
                 return client.send(new HeadObjectCommand({Bucket:BUCKET, Key: row}))
            }))
            summary.TotalObjects += batch.length
            summary.RestoreFinished += result.filter(i => i.Restore && i.Restore.split(',')[0] === 'ongoing-request="false"').length
            summary.RestoreStarted += result.filter(i => i.Restore && i.Restore.split(',')[0] === 'ongoing-request="true"').length
            console.log('heartbeat:',summary)
            return summary
        })
        return(nextBatch)
       //set up the summary object
    }, Promise.resolve({TotalObjects: 0, RestoreStarted: 0, RestoreFinished: 0}))
    //log the final results
    console.log(finalResults)
    //stop the timer
    console.timeEnd('How long did it take')
})()
