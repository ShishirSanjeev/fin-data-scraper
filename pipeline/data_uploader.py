import argparse
import os

import pysftp
import requests
from pathlib import Path

from utils.logger import get_logger

logger = get_logger("uploader", "data_upload.log")


class APIUploader:
    def __init__(self, api_base_url, api_key):
        self.api_base_url = api_base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def upload(self, csv_path):
        try:
            with open(csv_path, "r") as f:
                csv_data = f.read()

            payload = {
                "data": csv_data,
                "filename": Path(csv_path).name,
                "timestamp": os.path.getmtime(csv_path),
            }
            resp = requests.post(
                f"{self.api_base_url}/api/financial-data/upload",
                headers=self.headers,
                json=payload,
            )
            if resp.status_code == 200:
                logger.info(f"Uploaded {csv_path}")
                return True
            logger.error(f"Upload failed [{resp.status_code}]: {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False


class SFTPUploader:
    def __init__(self, hostname, username, password=None, private_key_path=None):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None

    def _connect(self):
        kwargs = dict(host=self.hostname, username=self.username, cnopts=self.cnopts)
        if self.password:
            kwargs["password"] = self.password
        else:
            kwargs["private_key"] = self.private_key_path
        return pysftp.Connection(**kwargs)

    def upload(self, local_path, remote_path):
        try:
            with self._connect() as sftp:
                sftp.put(local_path, remote_path)
                logger.info(f"Uploaded {local_path} -> {remote_path} on {self.hostname}")
                return True
        except Exception as e:
            logger.error(f"SFTP upload failed: {e}")
            return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload financial data")
    parser.add_argument("--method", choices=["api", "sftp"], required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--api_url")
    parser.add_argument("--api_key")
    parser.add_argument("--hostname")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--key_path")
    parser.add_argument("--remote_path")
    args = parser.parse_args()

    if args.method == "api":
        if not args.api_url or not args.api_key:
            parser.error("--api_url and --api_key are required for api method")
        uploader = APIUploader(args.api_url, args.api_key)
        ok = uploader.upload(args.input)

    elif args.method == "sftp":
        if not all([args.hostname, args.username, args.remote_path]):
            parser.error("--hostname, --username and --remote_path are required for sftp method")
        if not args.password and not args.key_path:
            parser.error("Either --password or --key_path is required for sftp method")
        uploader = SFTPUploader(args.hostname, args.username, args.password, args.key_path)
        ok = uploader.upload(args.input, args.remote_path)

    print("Success" if ok else "Failed")
