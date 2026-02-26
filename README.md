# S3 Api Wrapper

* This a pretty simple s3 wrapper, it uses boto3 and aiohttp and is made for mostly personal usage.
* I'm gonna redo this eventually becuase I saw an aioboto3 which would make it so i don't have to do
the presigned urls for this cause doesn't need it otherwise.

# s3Api

Main class for s3 interactions, allows uploads, downloads, deletion and file utilities

downloadFile

Takes in destination and s3path, uses generate_presigned_url and aiohttp, downloads a file in s3 to the destination
Can be used on any file but meant for those below 10MB, use downloadMultipart for anything above

downloadMultipart

Takes in destination and s3path
Drastically faster downloadFile as it breaks it into chunks and uses aysnc to download them all much quicker, but it's kinda the same besides chunking and managing the download session
Please use over downloadFile for files above 10MB
You could use it for all files as it goes to downloadFile if it's 10MB or less but don't it's just extra computation

uploadFile

Takes in local file and s3path
Uploads a file with the same methods as the rest, generate_presigned_url and aiohttp
Same as downloadFile with the fact that it's not meant for big files, use uploadMultipart instead

uploadMultipart

Takes in local file and s3path
Uploads a file but in chunks, it's just the revserse of downloadMultipart
Use for files over 10MB
