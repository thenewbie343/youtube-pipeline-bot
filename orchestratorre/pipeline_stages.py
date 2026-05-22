"""
Pipeline Manager — State, Google Drive, Colab Trigger
"""

import os, json, time, aiohttp, asyncio, logging
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io, tempfile

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/youtube"
]

class PipelineManager:

    def __init__(self):
        sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        sa_info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        self.drive = build("drive", "v3", credentials=creds)
        self.root_folder = self._ensure_folder(os.environ.get("DRIVE_FOLDER_ROOT", "YouTube_Pipeline"))

    # ── Drive helpers ────────────────────────────────────────────────────────

    def _ensure_folder(self, name, parent_id=None):
        query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        res = self.drive.files().list(q=query, fields="files(id)").execute()
        files = res.get("files", [])
        if files:
            return files[0]["id"]
        meta = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_id:
            meta["parents"] = [parent_id]
        f = self.drive.files().create(body=meta, fields="id").execute()
        return f["id"]

    def _write_json(self, folder_id, filename, data):
        content = json.dumps(data, indent=2).encode()
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        existing = self.drive.files().list(q=query, fields="files(id)").execute().get("files", [])
        if existing:
            self.drive.files().update(
                fileId=existing[0]["id"],
                media_body=MediaFileUpload(
                    self._tmp_write(content), mimetype="application/json"
                )
            ).execute()
            return existing[0]["id"]
        else:
            meta = {"name": filename, "parents": [folder_id]}
            f = self.drive.files().create(
                body=meta,
                media_body=MediaFileUpload(
                    self._tmp_write(content), mimetype="application/json"
                ),
                fields="id"
            ).execute()
            return f["id"]

    def _tmp_write(self, content: bytes) -> str:
        t = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        t.write(content)
        t.flush()
        return t.name

    def _read_json(self, folder_id, filename):
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        files = self.drive.files().list(q=query, fields="files(id)").execute().get("files", [])
        if not files:
            return None
        fid = files[0]["id"]
        buf = io.BytesIO()
        dl = MediaIoBaseDownload(buf, self.drive.files().get_media(fileId=fid))
        done = False
        while not done:
            _, done = dl.next_chunk()
        return json.loads(buf.getvalue())

    # ── Project lifecycle ────────────────────────────────────────────────────

    def create_project(self, topic, description, publish_time, chat_id) -> str:
        project_id = f"Project_{int(time.time())}"
        folder_id  = self._ensure_folder(project_id, self.root_folder)

        # Sub-folders
        for sub in ["ManualAssets", "Audio", "Video", "Research"]:
            self._ensure_folder(sub, folder_id)

        checkpoint = {
            "project_id":   project_id,
            "folder_id":    folder_id,
            "topic":        topic,
            "description":  description,
            "publish_time": publish_time,
            "chat_id":      chat_id,
            "stage":        1,
            "stage_name":   "research",
            "status":       "queued",
            "created_at":   datetime.utcnow().isoformat(),
            "paused_scene": None,
            "error":        None,
            "script":       None,
            "scenes":       [],
        }
        self._write_json(folder_id, "checkpoint.json", checkpoint)
        return project_id

    def get_checkpoint(self, project_id=None):
        """Get the most recent active project checkpoint."""
        # List all project folders
        query = (
            f"'{self.root_folder}' in parents and "
            "mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        folders = self.drive.files().list(
            q=query, orderBy="createdTime desc", fields="files(id,name)"
        ).execute().get("files", [])

        for folder in folders:
            if project_id and folder["name"] != project_id:
                continue
            cp = self._read_json(folder["id"], "checkpoint.json")
            if cp and cp.get("status") not in ("complete", "rejected"):
                return cp
        return None

    def get_status_report(self) -> str:
        cp = self.get_checkpoint()
        if not cp:
            return "💤 *No active pipeline.*\n\nSend /start to create a new project."

        stage_icons = {
            "research": "🔍", "scripting": "✍️", "audio": "🎙️",
            "assets": "🖼️", "assembly": "🎬", "qc": "🔬", "upload": "📤"
        }
        icon = stage_icons.get(cp.get("stage_name", ""), "⚙️")
        status_map = {
            "queued":     "🟡 Queued",
            "running":    "🟢 Running",
            "paused":     "⏸️ Paused — waiting for your asset",
            "error":      "🔴 Error",
            "complete":   "✅ Complete",
        }
        status = status_map.get(cp.get("status", ""), cp.get("status", ""))

        lines = [
            f"📊 *Pipeline Status*",
            f"",
            f"📁 *Project:* `{cp['project_id']}`",
            f"🎬 *Topic:* {cp['topic']}",
            f"",
            f"{icon} *Stage {cp.get('stage',1)}/8:* {cp.get('stage_name','').title()}",
            f"🔵 *Status:* {status}",
        ]

        if cp.get("paused_scene"):
            lines.append(f"\n⚠️ *Needs asset for:* Scene {cp['paused_scene']}")
            lines.append("Upload the `.mp4` file here, then send /resume")

        if cp.get("error"):
            lines.append(f"\n❌ *Last error:* `{cp['error'][:200]}`")

        return "\n".join(lines)

    def list_projects(self) -> str:
        query = (
            f"'{self.root_folder}' in parents and "
            "mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        folders = self.drive.files().list(
            q=query, orderBy="createdTime desc",
            fields="files(id,name,createdTime)"
        ).execute().get("files", [])

        if not folders:
            return "📁 No projects yet."

        lines = ["📁 *All Projects*\n"]
        for f in folders[:10]:
            cp = self._read_json(f["id"], "checkpoint.json")
            if cp:
                lines.append(
                    f"• `{f['name']}` — {cp.get('status','?')} "
                    f"(Stage {cp.get('stage',1)}/8)"
                )
        return "\n".join(lines)

    def resume_paused_pipeline(self) -> str:
        cp = self.get_checkpoint()
        if not cp:
            return "❌ No active project found."
        if cp.get("status") != "paused":
            return f"ℹ️ Project is `{cp.get('status')}`, not paused."

        # Clear pause, let Colab pick it up
        cp["status"] = "running"
        cp["paused_scene"] = None
        folder_id = cp["folder_id"]
        self._write_json(folder_id, "checkpoint.json", cp)
        return "▶️ Pipeline resumed! Colab will pick up in the next poll cycle."

    async def receive_manual_asset(self, tg_file, file_name: str) -> str:
        """Download asset from Telegram and save to Drive ManualAssets folder."""
        cp = self.get_checkpoint()
        if not cp:
            return "❌ No active project to attach this to."

        folder_id = cp["folder_id"]
        # Find ManualAssets sub-folder
        query = (
            f"name='ManualAssets' and '{folder_id}' in parents and "
            "mimeType='application/vnd.google-apps.folder'"
        )
        res = self.drive.files().list(q=query, fields="files(id)").execute()
        assets_folder = res["files"][0]["id"] if res["files"] else folder_id

        # Download from Telegram
        tmp_path = f"/tmp/{file_name}"
        await tg_file.download_to_drive(tmp_path)

        # Upload to Drive
        meta = {"name": file_name, "parents": [assets_folder]}
        self.drive.files().create(
            body=meta,
            media_body=MediaFileUpload(tmp_path, resumable=True),
            fields="id"
        ).execute()

        return (
            f"✅ Asset `{file_name}` uploaded to Drive!\n\n"
            f"Send /resume to continue the pipeline."
        )

    # ── Colab trigger ────────────────────────────────────────────────────────

    async def trigger_colab_worker(self, project_id: str, chat_id: int):
        """POST to Colab's ngrok URL to kick off processing."""
        colab_url = os.environ.get("COLAB_TRIGGER_URL", "")
        if not colab_url:
            logger.warning("COLAB_TRIGGER_URL not set — Colab must be started manually.")
            return

        secret = os.environ.get("PIPELINE_SECRET", "")
        payload = {"project_id": project_id, "secret": secret, "chat_id": chat_id}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{colab_url}/run",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        logger.info(f"Colab triggered for {project_id}")
                    else:
                        logger.error(f"Colab trigger failed: {resp.status}")
        except Exception as e:
            logger.error(f"Colab trigger error: {e}")
