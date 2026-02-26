import os
import asyncio
from xml.etree.ElementTree import *

from s3Api.s3_api import S3Api

class Executor:
    def __init__(self):
        self.api = S3Api()

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