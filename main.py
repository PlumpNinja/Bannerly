import httpx # New library for async requests
from fastapi import FastAPI, HTTPException, Query, Path, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError
import uvicorn
import json
import os
import time 
import io 
import base64 
from PIL import Image, ImageOps 
import asyncio 
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone 
import traceback 

from typing import List, Optional, Dict, Any, Literal, Set

# --- Configuration ---
DATA_DIR = "data"
CONFIG_FILE_NAME = "config.json"
BANNERED_DB_FILE_NAME = "BanneredDB.json"
CONFIG_FILE_PATH = os.path.join(DATA_DIR, CONFIG_FILE_NAME)
BANNERED_DB_FILE_PATH = os.path.join(DATA_DIR, BANNERED_DB_FILE_NAME)

BANNER_DIR = "Banners" 
BANNER_FILES = {
    "4K": "4K.png",
    "DV": "DV.png",
    "HDR": "HDR.png"
}
LOADED_BANNERS: Dict[str, Optional[Image.Image]] = {} 

MAX_RETRIES = 25 
RETRY_DELAY_SECONDS = 2
AUTOMATION_RESTART_DELAY_SECONDS = 6 * 60 * 60 # 6 hours

os.makedirs(DATA_DIR, exist_ok=True)
if not os.path.exists(BANNER_DIR):
    print(f"Warning: Banner directory '{BANNER_DIR}' not found. Banner functionality will be limited.")

# --- Pydantic Models (Unchanged) ---
class Credentials(BaseModel):
    jellyfin_host: Optional[str] = None; jellyfin_user_id: Optional[str] = None
    jellyfin_api_key: Optional[str] = None; tmdb_api_key: Optional[str] = None
class MaskedCredentials(BaseModel):
    jellyfin_host: Optional[str] = None; jellyfin_user_id_set: bool = False
    jellyfin_api_key_set: bool = False; tmdb_api_key_set: bool = False; are_all_set: bool = False
class MediaStream(BaseModel):
    Type: Optional[str] = None; DisplayTitle: Optional[str] = None; BitRate: Optional[int] = None
    VideoRange: Optional[str] = None; VideoRangeType: Optional[str] = None
    Height: Optional[int] = None; Width: Optional[int] = None  
class MovieItem(BaseModel): 
    Name: Optional[str] = None; ProductionYear: Optional[int] = None
    ProviderIds: Optional[Dict[str, str]] = {}; Id: Optional[str] = None 
    MediaStreams: List[MediaStream] = []; TmdbId: Optional[str] = None; JellyfinId: Optional[str] = None 
class PosterImageDetail(BaseModel):
    aspect_ratio: float; height: int; iso_639_1: Optional[str] = None; file_path: str
    vote_average: float; vote_count: int; width: int
class SetPosterRequest(BaseModel):
    tmdb_poster_path: str = Field(..., description="The file_path of the TMDB poster (e.g., /poster.jpg)")
class SetPosterResponse(BaseModel): 
    message: str; bannered_image_base64: Optional[str] = None 
class AutomationStatus(BaseModel):
    is_active: bool
    stop_requested: bool
    current_activity: str
    last_run_summary: Optional[str] = None; next_scheduled_run_time: Optional[str] = None 
SortByField = Literal["Name", "Resolution", "HDR", "Bitrate", "Year"]
SortOrder = Literal["asc", "desc"]

# --- Automation State Management (AsyncIO Version) ---
automation_state = {
    "is_active": False, 
    "current_activity": "Idle",
    "last_run_summary": "Automation has not run yet.",
    "next_scheduled_run_time": None, 
    "automation_task": None,
    "scheduler_task": None,
}
stop_event = asyncio.Event() 
state_lock = asyncio.Lock() 

# --- Lifespan function for startup/shutdown ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Lifespan: Application startup commencing...")
    _load_banner_images_api_sync() 
    print("Lifespan: Application startup complete. Banners loaded (if found).")
    yield 
    print("Lifespan: Application shutdown commencing...")
    # Get references to tasks without holding the lock
    task_to_cancel = automation_state.get("automation_task")
    scheduler_to_cancel = automation_state.get("scheduler_task")

    if (task_to_cancel and not task_to_cancel.done()) or \
       (scheduler_to_cancel and not scheduler_to_cancel.done()):
        print("Lifespan: Requesting active automation tasks to stop.")
        stop_event.set()
        if scheduler_to_cancel: scheduler_to_cancel.cancel()
        if task_to_cancel: task_to_cancel.cancel()
        
        # Give them a moment to acknowledge cancellation
        await asyncio.sleep(0.1)

    print("Lifespan: Application shutdown complete.")

# --- Basic App Setup ---
app = FastAPI(
    title="Bannerly Web API (Async)",
    description="API for managing Jellyfin movie posters with banners, using an asyncio backend.",
    version="0.4.6", # Version bump for Upload Fix
    lifespan=lifespan
)

# --- CORS Middleware (Unchanged) ---
origins = ["http://localhost", "http://localhost:8000", "http://127.0.0.1", "http://127.0.0.1:8000", "null", "*"]
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Helper Functions ---
def _load_full_credentials() -> Credentials: # Sync
    if not os.path.exists(CONFIG_FILE_PATH): return Credentials()
    try:
        with open(CONFIG_FILE_PATH, "r") as f: data = json.load(f)
        return Credentials(**data)
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Error loading or parsing credentials file: {e}"); return Credentials()

def _load_bannered_db() -> List[str]: # Sync
    if not os.path.exists(BANNERED_DB_FILE_PATH): return []
    try:
        with open(BANNERED_DB_FILE_PATH, "r") as f: data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e: print(f"Error loading {BANNERED_DB_FILE_NAME}: {e}. Using empty DB."); return []

def _save_bannered_db(jellyfin_ids: List[str], all_current_jellyfin_ids_in_library_set: Optional[set] = None): # Sync
    content_to_save = list(set(jellyfin_ids)) 
    if all_current_jellyfin_ids_in_library_set is not None:
        original_count = len(content_to_save)
        content_to_save = [item_id for item_id in content_to_save if item_id in all_current_jellyfin_ids_in_library_set]
        removed_count = original_count - len(content_to_save)
        if removed_count > 0:
            _automation_log_sync(f"DB Maintenance: Removed {removed_count} stale movie IDs from {BANNERED_DB_FILE_NAME}.", "INFO")
    try:
        with open(BANNERED_DB_FILE_PATH, "w") as f: json.dump(content_to_save, f, indent=4)
    except IOError as e: _automation_log_sync(f"ERROR saving to {BANNERED_DB_FILE_NAME}: {e}", "ERROR")

def _automation_log_sync(message: str, level: str = "INFO"):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")
    log_message = f"AUTOMATION [{timestamp}] [{level.upper()}]: {message}" 
    print(log_message, flush=True) 

async def _automation_log(message: str, level: str = "INFO"):
    _automation_log_sync(message, level)
    if level.upper() not in ["DEBUG", "TRACE"]: 
        async with state_lock: automation_state["current_activity"] = message

def _load_banner_images_api_sync():
    global LOADED_BANNERS
    if any(LOADED_BANNERS.values()): return
    _automation_log_sync("Loading banner images for API...", "DEBUG")
    for key, filename in BANNER_FILES.items():
        try:
            base_path = os.path.dirname(os.path.abspath(__file__))
            banner_path = os.path.join(base_path, BANNER_DIR, filename)
            if os.path.exists(banner_path):
                LOADED_BANNERS[key] = Image.open(banner_path).convert("RGBA")
            else:
                LOADED_BANNERS[key] = None; _automation_log_sync(f"Banner file NOT FOUND: {banner_path}", "WARN")
        except Exception as e:
            _automation_log_sync(f"Loading banner {key}: {e}", "ERROR"); LOADED_BANNERS[key] = None
    if not any(LOADED_BANNERS.values()): _automation_log_sync(f"Banner loading FAILED for all banners.", "WARN")

def _apply_banners_to_poster_api_sync(poster_pil_image: Image.Image, movie_item: MovieItem) -> Image.Image:
    _load_banner_images_api_sync()
    specs = get_movie_specs_for_banners_api(movie_item) 
    if not any(LOADED_BANNERS.values()) or not specs: return poster_pil_image
    
    _automation_log_sync(f"Applying banners for specs: {specs} to movie {movie_item.Name}", "DEBUG")
    final_image = poster_pil_image.copy().convert("RGBA")
    banner_application_order = ["4K", "DV", "HDR"] 
    for banner_key in banner_application_order:
        if banner_key in specs and LOADED_BANNERS.get(banner_key):
            banner_img_original = LOADED_BANNERS[banner_key]
            if banner_img_original:
                banner_img_resized = banner_img_original.resize(final_image.size, Image.Resampling.LANCZOS)
                final_image.paste(banner_img_resized, (0,0), banner_img_resized)
    return final_image

def get_resolution_sort_value(movie: MovieItem) -> int: #... (unchanged)
    for stream in movie.MediaStreams:
        if stream.Type == "Video" and stream.DisplayTitle:
            dt_upper = stream.DisplayTitle.upper()
            if "8K" in dt_upper: return 8000
            if "4K" in dt_upper or "2160P" in dt_upper: return 4000
            if "1080P" in dt_upper: return 1080; 
            if "720P" in dt_upper: return 720
            if "480P" in dt_upper: return 480
            if "SD" in dt_upper: return 400
        if stream.Type == "Video" and stream.Height is not None:
            if stream.Height >= 2000 : return 4000 
            if stream.Height >= 1000 : return 1080 
            if stream.Height >= 700 : return 720  
            if stream.Height >= 400 : return 480
    return 0
def get_hdr_sort_value(movie: MovieItem) -> int: #... (unchanged)
    order = {"DOVI": 7, "HDR10PLUS": 6, "HDR10": 5, "HLG": 4, "HDR": 3, "SDR": 1}
    highest_hdr_val = 0
    for stream in movie.MediaStreams:
        if stream.Type == "Video":
            if stream.VideoRangeType and stream.VideoRangeType.upper() in order:
                highest_hdr_val = max(highest_hdr_val, order[stream.VideoRangeType.upper()])
            if stream.DisplayTitle:
                dt_upper = stream.DisplayTitle.upper() 
                if "DOLBY VISION" in dt_upper or "DV" in dt_upper: highest_hdr_val = max(highest_hdr_val, order["DOVI"])
                elif "HDR10+" in dt_upper: highest_hdr_val = max(highest_hdr_val, order["HDR10PLUS"])
                elif "HDR10" in dt_upper: highest_hdr_val = max(highest_hdr_val, order["HDR10"])
                elif "HLG" in dt_upper: highest_hdr_val = max(highest_hdr_val, order["HLG"])
                elif "HDR" in dt_upper and not ("HDR10" in dt_upper or "HDR10+" in dt_upper):
                    highest_hdr_val = max(highest_hdr_val, order["HDR"])
            if stream.VideoRange and stream.VideoRange.upper() == "SDR":
                highest_hdr_val = max(highest_hdr_val, order["SDR"])
    return highest_hdr_val
def get_bitrate_sort_value(movie: MovieItem) -> float: #... (unchanged)
    for stream in movie.MediaStreams:
        if stream.Type == "Video" and stream.BitRate is not None: return stream.BitRate / 1_000_000 
    return 0.0
def get_movie_specs_for_banners_api(movie_item: MovieItem) -> Set[str]: #... (unchanged)
    specs = set()
    if not movie_item: return specs
    resolution_val = get_resolution_sort_value(movie_item)
    if resolution_val >= 4000: specs.add("4K")
    has_dv = False; has_hdr = False
    for stream in movie_item.MediaStreams:
        if stream.Type == "Video":
            vrt = (stream.VideoRangeType or "").upper(); dt = (stream.DisplayTitle or "").upper() 
            if "DOVI" in vrt or "DOLBY VISION" in dt or "DV" in dt: has_dv = True; break 
            if "HDR10" in vrt or "HDR10" in dt or "HLG" in vrt or "HLG" in dt or \
               "PQ" in vrt or "PQ" in dt or "HDR" in vrt or "HDR" in dt: has_hdr = True
    if has_dv: specs.add("DV")
    elif has_hdr: specs.add("HDR") 
    return specs

# --- ASYNC Automation Core ---

async def _get_jellyfin_item_details_async(client: httpx.AsyncClient, item_id: str, creds: Credentials) -> Optional[MovieItem]:
    if stop_event.is_set(): return None
    host = creds.jellyfin_host.rstrip('/')
    if not host.startswith(("http://", "https://")): host = "http://" + host
    url = f"{host}/Users/{creds.jellyfin_user_id}/Items/{item_id}"
    headers = {"X-Emby-Token": creds.jellyfin_api_key}
    params = {"Fields": "MediaStreams,ProviderIds,Id,ProductionYear,Name,VideoRangeType,VideoRange,Width,Height"}
    try:
        response = await client.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        movie = MovieItem(**response.json()); movie.JellyfinId = movie.Id
        if movie.ProviderIds: movie.TmdbId = movie.ProviderIds.get("Tmdb")
        return movie
    except httpx.HTTPStatusError as e: await _automation_log(f"Jellyfin API error for item {item_id}: {e}", "ERROR")
    except Exception as e: await _automation_log(f"Error fetching/parsing item details for {item_id}: {e}", "ERROR")
    return None

async def _get_best_tmdb_poster_url_for_automation_async(client: httpx.AsyncClient, tmdb_id: str, movie_name: str, creds: Credentials) -> Optional[str]:
    if not tmdb_id or not creds.tmdb_api_key:
        await _automation_log(f"Missing TMDB ID/Key for '{movie_name}'.", "WARN"); return None
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/images"
    params = {"api_key": creds.tmdb_api_key}
    for attempt in range(1, MAX_RETRIES + 1):
        if stop_event.is_set():
            await _automation_log(f"Stop event set, aborting TMDB fetch for {movie_name}.", "WARN"); return None
        try:
            if attempt > 1: await _automation_log(f"Retrying TMDB metadata for '{movie_name}' ({attempt}/{MAX_RETRIES})...", "INFO")
            response = await client.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json(); posters = data.get("posters", [])
            english_posters = sorted([p for p in posters if p.get("iso_639_1") == "en"], key=lambda p: p.get("vote_average", 0), reverse=True)
            if english_posters and english_posters[0].get("file_path"):
                return f"https://image.tmdb.org/t/p/original{english_posters[0].get('file_path')}"
            await _automation_log(f"No English poster found for '{movie_name}'.", "WARN"); return None
        except httpx.RequestError as e:
            await _automation_log(f"TMDB fetch attempt {attempt} failed for '{movie_name}': {e}", "WARN")
            if attempt < MAX_RETRIES: await asyncio.sleep(RETRY_DELAY_SECONDS)
            else: await _automation_log(f"Max retries reached for TMDB metadata on '{movie_name}'.", "ERROR"); return None
    return None

async def _upload_bannered_poster_to_jellyfin_async(client: httpx.AsyncClient, jid: str, b64_image_string: str, creds: Credentials) -> bool:
    if stop_event.is_set(): return False
    host = creds.jellyfin_host.rstrip('/')
    if not host.startswith(("http://", "https://")): host = "http://" + host
    url = f"{host}/Items/{jid}/Images/Primary"
    headers = {"X-Emby-Token": creds.jellyfin_api_key, "Content-Type": "image/png"}
    try:
        await _automation_log(f"Uploading to Jellyfin for item ID: {jid}", "DEBUG")
        # Use `data` to send the raw base64 string, not `content` which sends bytes
        response = await client.post(url, data=b64_image_string, headers=headers, timeout=30)
        response.raise_for_status()
        return True
    except Exception as e: await _automation_log(f"Jellyfin upload failed for {jid}: {e}", "ERROR"); return False

async def run_automation_cycle_async():
    global automation_state
    await _automation_log("Async automation cycle starting...")
    creds = _load_full_credentials() 
    if not all([creds.jellyfin_host, creds.jellyfin_user_id, creds.jellyfin_api_key, creds.tmdb_api_key]):
        await _automation_log("Automation cannot start: Missing credentials.", "CRITICAL")
        async with state_lock: automation_state["last_run_summary"] = "Failed: Credentials missing."
        return 

    try:
        async with httpx.AsyncClient() as client:
            await _automation_log("Fetching full movie list from Jellyfin...", "INFO")
            host = creds.jellyfin_host.rstrip('/'); 
            if not host.startswith(("http://", "https://")): host = "http://" + host
            url = f"{host}/Users/{creds.jellyfin_user_id}/Items"
            headers = {"X-Emby-Token": creds.jellyfin_api_key}
            params = {"Recursive": "true", "IncludeItemTypes": "Movie", "Fields": "MediaStreams,ProviderIds,Id,ProductionYear,Name"}
            
            response = await client.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status(); data = response.json()
            
            all_movies_from_jellyfin: List[MovieItem] = []
            all_fetched_ids_for_db_cleaning = set()
            for item_data in data.get("Items", []):
                if item_data.get("Id"): all_fetched_ids_for_db_cleaning.add(item_data.get("Id"))
                try:
                    movie = MovieItem(**item_data); movie.JellyfinId = movie.Id
                    if movie.ProviderIds: movie.TmdbId = movie.ProviderIds.get("Tmdb")
                    all_movies_from_jellyfin.append(movie)
                except Exception as e: await _automation_log(f"Skipping item due to parsing error: {item_data.get('Name', 'Unknown')}. Error: {e}", "WARN")

            if stop_event.is_set(): raise asyncio.CancelledError()

            await _automation_log("Managing BanneredDB...", "INFO")
            bannered_db_ids = await asyncio.to_thread(_load_bannered_db)
            await asyncio.to_thread(_save_bannered_db, bannered_db_ids, all_fetched_ids_for_db_cleaning)
            bannered_db_ids = await asyncio.to_thread(_load_bannered_db) 

            new_movies_to_process = [m for m in all_movies_from_jellyfin if m.JellyfinId and m.JellyfinId not in bannered_db_ids and m.TmdbId]

            if not new_movies_to_process:
                summary = "Automation cycle complete: No new movies to process."
                async with state_lock: automation_state["last_run_summary"] = summary
                await _automation_log(summary, "INFO")
            else:
                await _automation_log(f"Found {len(new_movies_to_process)} new movies to process.", "INFO")
                success_count, fail_count = 0, 0
                for index, movie in enumerate(new_movies_to_process):
                    if stop_event.is_set(): break
                    movie_name = movie.Name or "Unknown"
                    await _automation_log(f"Processing ({index + 1}/{len(new_movies_to_process)}): '{movie_name}'", "INFO")

                    details = await _get_jellyfin_item_details_async(client, movie.JellyfinId, creds)
                    if not details: fail_count += 1; continue
                    
                    poster_url = await _get_best_tmdb_poster_url_for_automation_async(client, details.TmdbId, movie_name, creds)
                    if not poster_url: fail_count += 1; continue

                    img_response = await client.get(poster_url, timeout=15)
                    img_response.raise_for_status()
                    original_pil_image = await asyncio.to_thread(Image.open, io.BytesIO(img_response.content))
                    
                    final_image_pil = await asyncio.to_thread(_apply_banners_to_poster_api_sync, original_pil_image, details)
                    
                    img_byte_arr = io.BytesIO()
                    await asyncio.to_thread(final_image_pil.save, img_byte_arr, format='PNG')
                    final_image_bytes = img_byte_arr.getvalue()
                    
                    # MODIFIED: Create base64 string for upload
                    b64_image_string = base64.b64encode(final_image_bytes).decode('utf-8')
                    
                    if await _upload_bannered_poster_to_jellyfin_async(client, details.JellyfinId, b64_image_string, creds):
                        success_count += 1; await _automation_log(f"Successfully set poster for '{movie_name}'.", "SUCCESS")
                        bannered_db_ids.append(details.JellyfinId)
                        await asyncio.to_thread(_save_bannered_db, bannered_db_ids, all_fetched_ids_for_db_cleaning)
                    else:
                        fail_count += 1
                    
                    if stop_event.is_set(): break
                    await asyncio.sleep(RETRY_DELAY_SECONDS / 10) 

                summary = f"Automation cycle: Processed {success_count + fail_count}/{len(new_movies_to_process)}. Successful: {success_count}, Failed: {fail_count}."
                if stop_event.is_set(): summary += " Cycle was stopped by user."
                async with state_lock: automation_state["last_run_summary"] = summary
                await _automation_log(summary, "SUCCESS" if fail_count == 0 else "WARN")

    except asyncio.CancelledError:
        summary = "Automation cycle cancelled by user."
        async with state_lock: automation_state["last_run_summary"] = summary
        await _automation_log(summary, "WARN")
    except Exception as e:
        summary = f"Automation cycle failed with an unexpected error: {e}"
        async with state_lock: automation_state["last_run_summary"] = summary
        await _automation_log(f"CRITICAL ERROR in automation: {e}", "CRITICAL")
        await _automation_log(traceback.format_exc(), "ERROR")
    finally:
        await _automation_log("Automation cycle ended. Cleaning up state.", "INFO")
        async with state_lock:
            automation_state["is_active"] = False
            automation_state["current_activity"] = "Idle"
            automation_state["automation_task"] = None
            if not stop_event.is_set(): 
                await _automation_log(f"Scheduling next run in {AUTOMATION_RESTART_DELAY_SECONDS} seconds.", "INFO")
                automation_state["scheduler_task"] = asyncio.create_task(schedule_next_automation())
            
async def schedule_next_automation():
    await asyncio.sleep(AUTOMATION_RESTART_DELAY_SECONDS)
    async with state_lock:
        if stop_event.is_set():
            await _automation_log("Scheduler: Stop event is set, not starting new cycle.", "WARN")
            return
    await _automation_log("Scheduler: Timer finished, requesting new automation cycle.", "INFO")
    try:
        port = os.getenv("PORT", "8000") 
        async with httpx.AsyncClient() as client:
            await client.post(f"http://127.0.0.1:{port}/api/automation/start", timeout=10)
    except Exception as e:
        await _automation_log(f"Scheduler failed to re-trigger automation: {e}", "ERROR")

# --- API Endpoints ---
@app.post("/api/automation/start", tags=["Automation"])
async def start_automation_endpoint():
    async with state_lock:
        if automation_state["is_active"]:
            raise HTTPException(status_code=409, detail="Automation is already running.")
        if automation_state["scheduler_task"] and not automation_state["scheduler_task"].done():
            automation_state["scheduler_task"].cancel()
            await _automation_log("Manual start; pending scheduled run cancelled.", "INFO")
        
        stop_event.clear() 
        automation_state["is_active"] = True
        automation_state["current_activity"] = "Initiating automation cycle..."
        automation_state["automation_task"] = asyncio.create_task(run_automation_cycle_async())
    
    _automation_log_sync("Automation start request processed.", "INFO")
    return {"message": "Automation process initiated in the background."}

@app.post("/api/automation/stop", tags=["Automation"])
async def stop_automation_endpoint():
    # Get task references first, without holding the lock for a long time
    async with state_lock:
        task = automation_state.get("automation_task")
        scheduler = automation_state.get("scheduler_task")
        if not (task and not task.done()) and not (scheduler and not scheduler.done()):
            raise HTTPException(status_code=400, detail="Automation is not running or scheduled.")
    
    # Signal and cancel tasks outside the main state lock to prevent deadlock
    stop_event.set()
    if scheduler and not scheduler.done():
        scheduler.cancel()
        await _automation_log("Pending scheduled run cancelled by user.", "INFO")
    
    if task and not task.done():
        task.cancel() 
        await _automation_log("Stop request sent to active automation cycle.", "WARN")
        try:
            # Wait for the task to finish its cleanup (its finally block)
            await task 
        except asyncio.CancelledError:
            await _automation_log("Automation task acknowledged cancellation.", "INFO")

    # Final state update after task is confirmed done/cancelled
    async with state_lock:
        automation_state["current_activity"] = "Idle - Stopped"
        if automation_state.get("last_run_summary") and "cancelled" not in automation_state["last_run_summary"]:
             automation_state["last_run_summary"] += " (Cancelled)"

    return {"message": "Automation stop acknowledged and processed."}


@app.get("/api/automation/status", response_model=AutomationStatus, tags=["Automation"])
async def get_automation_status():
    async with state_lock:
        next_run_time_str = None
        if automation_state.get("next_scheduled_run_time"):
            try: next_run_time_str = automation_state["next_scheduled_run_time"].isoformat()
            except: pass
        
        return AutomationStatus(
            is_active=automation_state["is_active"],
            stop_requested=stop_event.is_set(),
            current_activity=automation_state["current_activity"],
            last_run_summary=automation_state["last_run_summary"],
            next_scheduled_run_time=next_run_time_str
        )

# --- Synchronous API Endpoints (for frontend that uses requests) ---
@app.get("/api/health", tags=["System"])
async def health_check():
    return {"status": "ok", "message": "Bannerly API is running"}

@app.post("/api/config", status_code=201, tags=["Configuration"])
async def save_app_credentials(creds: Credentials):
    if not all([creds.jellyfin_host, creds.jellyfin_user_id, creds.jellyfin_api_key, creds.tmdb_api_key]):
        raise HTTPException(status_code=400, detail="All credential fields are required.")
    try:
        await asyncio.to_thread(json.dump, creds.model_dump(), open(CONFIG_FILE_PATH, "w"), indent=4)
        return {"message": "Credentials saved successfully."}
    except Exception as e:
        print(f"Error saving credentials: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred while saving credentials.")

@app.get("/api/config", response_model=MaskedCredentials, tags=["Configuration"])
async def load_app_credentials_masked():
    creds = await asyncio.to_thread(_load_full_credentials)
    all_set = bool(creds.jellyfin_host and creds.jellyfin_user_id and creds.jellyfin_api_key and creds.tmdb_api_key)
    return MaskedCredentials(jellyfin_host=creds.jellyfin_host, jellyfin_user_id_set=bool(creds.jellyfin_user_id),
        jellyfin_api_key_set=bool(creds.jellyfin_api_key), tmdb_api_key_set=bool(creds.tmdb_api_key), are_all_set=all_set)

@app.delete("/api/config", tags=["Configuration"])
async def purge_app_credentials():
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            await asyncio.to_thread(os.remove, CONFIG_FILE_PATH)
            return {"message": "Credentials purged successfully."}
        except Exception as e:
            print(f"Error purging credentials: {e}")
            raise HTTPException(status_code=500, detail="An unexpected error occurred while purging credentials.")
    return {"message": "No credentials file to purge."}

async def _get_movies_sync_logic(search: Optional[str], sort_by: SortByField, sort_order: SortOrder):
    creds = _load_full_credentials()
    if not all([creds.jellyfin_host, creds.jellyfin_user_id, creds.jellyfin_api_key]):
        raise HTTPException(status_code=401, detail="Jellyfin credentials are not configured or incomplete.")
    
    async with httpx.AsyncClient() as client:
        host = creds.jellyfin_host.rstrip('/')
        if not host.startswith(("http://", "https://")): host = "http://" + host
        url = f"{host}/Users/{creds.jellyfin_user_id}/Items"
        headers = {"X-Emby-Token": creds.jellyfin_api_key}
        params = {"Recursive": "true", "IncludeItemTypes": "Movie", "Fields": "MediaStreams,ProductionYear,ProviderIds,Id,Name"}
        
        try:
            response = await client.get(url, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            data = response.json(); raw_items = data.get("Items", [])
            
            # DB Maintenance Logic
            all_fetched_ids_for_db_cleaning = {item.get("Id") for item in raw_items if item.get("Id")}
            if all_fetched_ids_for_db_cleaning:
                _automation_log_sync("Performing DB maintenance during manual movie fetch...", "DEBUG")
                bannered_db_ids = await asyncio.to_thread(_load_bannered_db)
                await asyncio.to_thread(_save_bannered_db, bannered_db_ids, all_fetched_ids_for_db_cleaning)

            processed_movies: List[MovieItem] = []
            for item_data in raw_items: 
                try: 
                    movie = MovieItem(**item_data); movie.JellyfinId = movie.Id
                    if movie.ProviderIds: movie.TmdbId = movie.ProviderIds.get("Tmdb")
                    if search:
                        if movie.Name and search.lower() in movie.Name.lower(): processed_movies.append(movie)
                    else: processed_movies.append(movie)
                except Exception as item_e: print(f"Warning: Could not parse movie item: {item_data.get('Name', 'Unknown')}. Error: {item_e}")
            
            reverse_sort = (sort_order == "desc")
            if sort_by == "Name": processed_movies.sort(key=lambda m: (m.Name or "").lower(), reverse=reverse_sort)
            elif sort_by == "Year": processed_movies.sort(key=lambda m: m.ProductionYear or 0, reverse=reverse_sort)
            elif sort_by == "Resolution": processed_movies.sort(key=get_resolution_sort_value, reverse=reverse_sort)
            elif sort_by == "HDR": processed_movies.sort(key=get_hdr_sort_value, reverse=reverse_sort)
            elif sort_by == "Bitrate": processed_movies.sort(key=get_bitrate_sort_value, reverse=reverse_sort)
            
            return processed_movies
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401: raise HTTPException(status_code=401, detail="Jellyfin authentication failed.")
            raise HTTPException(status_code=e.response.status_code, detail=f"Jellyfin API HTTP Error: {e.response.reason_phrase}")
        except httpx.RequestError as e: raise HTTPException(status_code=503, detail=f"Could not connect to Jellyfin: {e}")

@app.get("/api/movies", response_model=List[MovieItem], tags=["Movies"])
async def get_movies(search: Optional[str] = Query(None), sort_by: Optional[SortByField] = Query("Name"), sort_order: Optional[SortOrder] = Query("asc")):
    return await _get_movies_sync_logic(search, sort_by, sort_order)


@app.get("/api/movies/{tmdb_id}/posters", response_model=List[PosterImageDetail], tags=["Posters"])
async def get_movie_posters(tmdb_id: str = Path(..., description="The Movie Database ID of the movie.")):
    creds = _load_full_credentials()
    if not creds.tmdb_api_key: raise HTTPException(status_code=401, detail="TMDB API key is not configured.")
    tmdb_api_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/images"
    params = {"api_key": creds.tmdb_api_key}
    
    # MODIFIED: Added retry logic for manual poster fetch
    async with httpx.AsyncClient() as client:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if attempt > 1:
                    await _automation_log(f"Retrying manual poster fetch for TMDB ID {tmdb_id} (Attempt {attempt}/{MAX_RETRIES})", "INFO")
                
                response = await client.get(tmdb_api_url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json(); all_posters = data.get("posters", [])
                
                valid_posters: List[PosterImageDetail] = []
                if isinstance(all_posters, list):
                    english_posters_data = [p for p in all_posters if p.get("iso_639_1") == "en" and p.get("file_path")]
                    for p_data in english_posters_data:
                        try:
                            valid_posters.append(PosterImageDetail(**p_data))
                        except ValidationError as e:
                            print(f"Warning: Could not parse poster data for TMDB ID {tmdb_id}: {p_data}. Error: {e}")
                
                valid_posters.sort(key=lambda p: p.vote_average, reverse=True)
                return valid_posters # Success, exit the function

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404: raise HTTPException(status_code=404, detail=f"Movie with TMDB ID {tmdb_id} not found.")
                # For other HTTP errors, log and prepare to retry
                await _automation_log(f"TMDB API error on manual fetch for {tmdb_id} (Attempt {attempt}): {e}", "WARN")
                if attempt >= MAX_RETRIES: raise HTTPException(status_code=503, detail=f"TMDB API error after {MAX_RETRIES} attempts: {e.response.text}")

            except httpx.RequestError as e:
                await _automation_log(f"Network error on manual fetch for {tmdb_id} (Attempt {attempt}): {e}", "WARN")
                if attempt >= MAX_RETRIES: raise HTTPException(status_code=503, detail=f"Could not connect to TMDB after {MAX_RETRIES} attempts.")

            except Exception as e:
                await _automation_log(f"Unexpected error on manual fetch for {tmdb_id} (Attempt {attempt}): {e}", "ERROR")
                if attempt >= MAX_RETRIES: raise HTTPException(status_code=500, detail="An unexpected error occurred.")

            # If we are here, an error occurred and we need to retry
            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY_SECONDS)

    # This part should not be reached if MAX_RETRIES > 0, but as a fallback
    raise HTTPException(status_code=500, detail="Failed to fetch posters after all retries.")


@app.post("/api/movies/{jellyfin_id}/set-bannered-poster", response_model=SetPosterResponse, tags=["Posters"])
async def set_movie_poster_with_banners(jellyfin_id: str = Path(...), request_body: SetPosterRequest = Body(...)):
    creds = _load_full_credentials()
    if not all([creds.jellyfin_host, creds.jellyfin_user_id, creds.jellyfin_api_key, creds.tmdb_api_key]):
        raise HTTPException(status_code=401, detail="All credentials must be configured.")
    
    async with httpx.AsyncClient() as client:
        movie_item = await _get_jellyfin_item_details_async(client, jellyfin_id, creds)
        if not movie_item:
            raise HTTPException(status_code=404, detail=f"Movie with Jellyfin ID {jellyfin_id} not found.")

        tmdb_poster_full_url = f"https://image.tmdb.org/t/p/original{request_body.tmdb_poster_path}"
        try:
            img_response = await client.get(tmdb_poster_full_url, timeout=15)
            img_response.raise_for_status()
            
            original_pil_image = await asyncio.to_thread(Image.open, io.BytesIO(img_response.content))
            final_image_pil = await asyncio.to_thread(_apply_banners_to_poster_api_sync, original_pil_image, movie_item)

            img_byte_arr = io.BytesIO()
            await asyncio.to_thread(final_image_pil.save, img_byte_arr, format='PNG')
            image_bytes = img_byte_arr.getvalue()
            
            # MODIFIED: Create base64 string for upload
            b64_image_string = base64.b64encode(image_bytes).decode('utf-8')
            
            if not await _upload_bannered_poster_to_jellyfin_async(client, jellyfin_id, b64_image_string, creds):
                 raise HTTPException(status_code=500, detail="Failed to upload poster to Jellyfin.")
            
            bannered_db_ids = await asyncio.to_thread(_load_bannered_db)
            if jellyfin_id not in bannered_db_ids:
                bannered_db_ids.append(jellyfin_id)
                await asyncio.to_thread(_save_bannered_db, bannered_db_ids)
            
            return SetPosterResponse(
                message=f"Poster for '{movie_item.Name}' set successfully with banners.",
                bannered_image_base64=b64_image_string
            )
        except httpx.RequestError as e: raise HTTPException(status_code=503, detail=f"Failed to download poster from TMDB: {e}")
        except Exception as e: raise HTTPException(status_code=500, detail=f"Error processing poster: {e}")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

