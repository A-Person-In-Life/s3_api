import os
import boto3
import asyncio
import aiohttp
import aiofiles
import math
import requests
from xml.etree.ElementTree import *
import time


class S3Api:
    def __init__(self, authPath, region="us-east-2"):
        with open(authPath, "r") as f:
            self.accessKey = f.readline().strip()
            self.secretKey = f.readline().strip()
            self.bucketName = f.readline().strip()

        self.timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=60)
        self.session = None
        self.region = region
        self.client = boto3.client(
            "s3",
            aws_access_key_id=self.accessKey,
            aws_secret_access_key=self.secretKey,
            region_name=self.region
        )
        self.executor = Executor(self)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def downloadPart(self, partNumber, url, totalParts, fileName):
        async with self.session.get(url) as response:
            print(f"Downloaded Part {partNumber} out of {totalParts} for {fileName}")
            return {"PartNumber": partNumber, "Data": await response.read()}

    async def downloadMultipart(self, localDestination, s3Path):
        partSize = 5242880 * 2
        fileSize = int(self.getMetaData(s3Path, "Content-Length"))
        totalParts = math.ceil(fileSize / partSize)
        partUrls = []
        orderedParts = [None] * totalParts

        if totalParts == 1:
            await self.downloadFile(localDestination, s3Path)
            return

        for i in range(totalParts):
            start = i * partSize
            end = min(start + partSize - 1, fileSize - 1)
            url = self.client.generate_presigned_url(
                ClientMethod="get_object",
                Params={
                    "Bucket": self.bucketName,
                    "Key": s3Path,
                    "Range": f"bytes={start}-{end}"
                },
                ExpiresIn=3600
            )
            partUrls.append(url)

        tasks = [
            self.downloadPart(i + 1, partUrls[i], totalParts, os.path.basename(s3Path))
            for i in range(totalParts)
        ]
        parts = await asyncio.gather(*tasks)

        for part in parts:
            orderedParts[part["PartNumber"] - 1] = part["Data"]

        async with aiofiles.open(localDestination, "wb") as f:
            for part in orderedParts:
                await f.write(part)

        print(f"Multipart download completed for {s3Path}")

    async def downloadFile(self, localDestination, s3Path):
        url = self.client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": self.bucketName, "Key": s3Path},
            ExpiresIn=30
        )
        async with self.session.get(url) as response:
            async with aiofiles.open(localDestination, "wb") as f:
                await f.write(await response.read())
                print(f"DownloadFile, {s3Path}, {response.status}")

    async def uploadFile(self, localPath, s3Path):
        url = self.client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": self.bucketName, "Key": s3Path},
            ExpiresIn=30
        )
        async with aiofiles.open(localPath, "rb") as f:
            data = await f.read()
            async with self.session.put(url, data=data) as response:
                print(f"UploadFile, {s3Path}, {response.status}")

    async def uploadPart(self, localPath, partNumber, url, data, totalParts):
        async with self.session.put(url, data=data) as response:
            print(f"Uploaded Part {partNumber} out of {totalParts} for {os.path.basename(localPath)}")
            return {"ETag": response.headers["ETag"], "PartNumber": partNumber}

    async def uploadMultipart(self, localPath, s3Path):
        partSize = 5242880
        fileSize = os.path.getsize(localPath)
        totalParts = math.ceil(fileSize / partSize)
        parts_data = []
        partUrls = []

        if not s3Path:
            s3Path = os.path.basename(localPath)

        response = self.client.create_multipart_upload(Bucket=self.bucketName, Key=s3Path)
        uploadId = response["UploadId"]

        for i in range(1, totalParts + 1):
            url = self.client.generate_presigned_url(
                ClientMethod="upload_part",
                Params={"Bucket": self.bucketName, "Key": s3Path, "UploadId": uploadId, "PartNumber": i},
                ExpiresIn=3600
            )
            partUrls.append(url)

        async with aiofiles.open(localPath, "rb") as f:
            for _ in range(totalParts):
                parts_data.append(await f.read(partSize))

        tasks = [
            self.uploadPart(localPath, i + 1, partUrls[i], parts_data[i], totalParts)
            for i in range(totalParts)
        ]
        endData = await asyncio.gather(*tasks)

        self.client.complete_multipart_upload(
            Bucket=self.bucketName,
            Key=s3Path,
            UploadId=uploadId,
            MultipartUpload={"Parts": endData}
        )
        print(f"Multipart upload completed for {s3Path}")

    def listDir(self, s3Folder, operation=None):
        url = self.client.generate_presigned_url(
            ClientMethod="list_objects_v2",
            Params={"Bucket": self.bucketName, "Prefix": s3Folder, "Delimiter": "/"},
            ExpiresIn=30
        )
        response = requests.get(url)
        filenames = []
        subfolders = []

        nameSpace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
        xml = fromstring(response.content)

        for file in xml.findall("s3:Contents", nameSpace):
            filenames.append(file.find("s3:Key", nameSpace).text)
        for subfolder in xml.findall("s3:CommonPrefixes", nameSpace):
            subfolders.append(subfolder.find("s3:Prefix", nameSpace).text)

        print(filenames, subfolders)

        if operation == "folders":
            return subfolders
        elif operation == "files":
            return filenames
        return [filenames, subfolders]

    def getMetaData(self, s3File, operation):
        url = self.client.generate_presigned_url(
            ClientMethod="head_object",
            Params={"Bucket": self.bucketName, "Key": s3File},
            ExpiresIn=30
        )
        response = requests.head(url)
        print(response.headers)
        return response.headers[operation]

    async def push(self, localFolder, s3Folder):
        await self.executor.push(localFolder, s3Folder)

    async def pull(self, localFolder, s3Folder):
        await self.executor.pull(localFolder, s3Folder)


class Executor:
    def __init__(self, api):
        self.api = api

    async def push(self, localFolder, s3Folder):
        s3Filenames, s3Subfolders = self.api.listDir(s3Folder)
        s3Basenames = {os.path.basename(f) for f in s3Filenames}
        s3FolderBasenames = {os.path.basename(f.rstrip('/')) for f in s3Subfolders}

        tasks = []
        folders = []

        for entry in os.listdir(localFolder):
            entry_path = os.path.join(localFolder, entry)
            if os.path.isfile(entry_path) and entry not in s3Basenames:
                tasks.append(self.api.uploadMultipart(entry_path, s3Folder + entry))
            elif os.path.isdir(entry_path) and entry not in s3FolderBasenames:
                folders.append(entry)

        await asyncio.gather(*tasks)

        for folder in folders:
            await self.push(os.path.join(localFolder, folder), s3Folder + folder + "/")

    async def pull(self, localFolder, s3Folder):
        s3Filenames, s3Subfolders = self.api.listDir(s3Folder)
        tasks = []

        for filePath in s3Filenames:
            relativePath = os.path.relpath(filePath, s3Folder)
            localPath = os.path.join(localFolder, relativePath)
            if not os.path.isfile(localPath):
                tasks.append(self.api.downloadMultipart(localPath, filePath))

        await asyncio.gather(*tasks)

        for subfolderPath in s3Subfolders:
            localSubfolder = os.path.join(localFolder, os.path.basename(subfolderPath.rstrip('/')))
            os.makedirs(localSubfolder, exist_ok=True)
            await self.pull(localSubfolder, subfolderPath)